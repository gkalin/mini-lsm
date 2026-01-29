// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::fs::copy;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::builder::PathBufValueParser;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use nom::error;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::{concat_iterator::SstConcatIterator, merge_iterator::MergeIterator};
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => return self.full_compaction(l0_sstables, l1_sstables),
            CompactionTask::Simple(task) => return self.simple_compaction(task),

            _ => panic!("should not happen"),
        };
    }
    fn simple_compaction(&self, task: &SimpleLeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let lock_guard = self.state.read();
            Arc::clone(&lock_guard)
        };
        let upper_iters = task
            .upper_level_sst_ids
            .iter()
            .try_fold(Vec::new(), |mut acc, sst_id| {
                let sst = snapshot.sstables.get(sst_id).cloned()?;
                let sst_iter = SsTableIterator::create_and_seek_to_first(sst).ok()?;
                acc.push(Box::new(sst_iter));
                Some(acc)
            })
            .ok_or(anyhow!("failed to create iter"))?;
        let lower_tables: Vec<Arc<SsTable>> = task
            .lower_level_sst_ids
            .iter()
            .try_fold(Vec::new(), |mut acc, sst_id| {
                let sst = snapshot.sstables.get(sst_id).cloned()?;
                acc.push(sst);
                Some(acc)
            })
            .ok_or(anyhow!("failed to get lower level SST"))?;
        let lower_concat = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
        self.compact_iterator(
            TwoMergeIterator::create(MergeIterator::create(upper_iters), lower_concat)?,
            task.is_lower_level_bottom_level,
        )
    }

    fn compact_iterator(
        &self,
        mut merge_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut compacted_ssts = Vec::new();
        let mut prev = Vec::new();
        let mut build_last = false;
        while merge_iter.is_valid() {
            let (curr, value) = (merge_iter.key(), merge_iter.value());
            if !prev.is_empty() && prev == curr.raw_ref() {
                // skip duplicates
                merge_iter.next()?;
                continue;
            }
            if !value.is_empty() || !compact_to_bottom_level {
                sst_builder.add(curr, value);
                build_last = true;
                if sst_builder.estimated_size() >= self.options.target_sst_size {
                    self.make_new_sst(&mut compacted_ssts, sst_builder)?;
                    sst_builder = SsTableBuilder::new(self.options.block_size);
                    build_last = false;
                }
            }
            prev = curr.to_key_vec().into_inner();
            merge_iter.next()?;
        }
        if build_last {
            self.make_new_sst(&mut compacted_ssts, sst_builder)?;
        }
        Ok(compacted_ssts)
    }

    fn full_compaction(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let lock_guard = self.state.read();
            Arc::clone(&lock_guard)
        };
        let sstable_iters = l0_sstables
            .iter()
            .chain(l1_sstables.iter())
            .filter_map(|id| {
                let sstable = snapshot.sstables.get(id)?.clone();
                let iter = SsTableIterator::create_and_seek_to_first(sstable).ok()?;
                iter.is_valid().then_some(Box::new(iter))
            })
            .collect();
        self.compact_iterator(MergeIterator::create(sstable_iters), true)
    }
    fn make_new_sst(
        &self,
        compacted_ssts: &mut Vec<Arc<SsTable>>,
        sst_builder: SsTableBuilder,
    ) -> Result<()> {
        let id = self.next_sst_id();
        let path = self.path_of_sst(id);
        let table = sst_builder.build(id, Some(self.block_cache.clone()), path)?;
        compacted_ssts.push(Arc::new(table));
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0, l1) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0,
            l1_sstables: l1,
        };
        let compacted = self.compact(&compaction_task)?;
        let mut to_delete = Vec::new();
        {
            let state_lock = self.state_lock.lock();
            let mut state_guard = self.state.write();
            let new_state = Arc::make_mut(&mut state_guard);
            let CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } = &compaction_task
            else {
                unreachable!()
            };
            new_state.l0_sstables.retain(|&x| {
                if l0_sstables.contains(&x) {
                    to_delete.push(x);
                    false
                } else {
                    true
                }
            });
            new_state.levels[0].1.retain(|x| {
                if l1_sstables.contains(x) {
                    to_delete.push(*x);
                    false
                } else {
                    true
                }
            });
            let mut ids = Vec::new();
            for sstable in compacted {
                let id = sstable.sst_id();
                ids.push(id);
                new_state.sstables.insert(id, sstable);
                new_state.levels[0].1.push(id);
            }
            self.sync_dir()?;
            self.manifest
                .as_ref()
                .ok_or(anyhow::anyhow!("no manifest"))?
                .add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, ids),
                )?;
        };
        for id in to_delete {
            _ = std::fs::remove_file(self.path_of_sst(id));
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let lock_guard = self.state.read();
            Arc::clone(&lock_guard)
        };
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };
        let compacted = self.compact(&task)?;
        let ids: Vec<usize> = compacted.iter().map(|table| table.sst_id()).collect();
        let to_delete =
            {
                let state_lock = self.state_lock.lock();
                let mut current_state = self.state.write();
                let (mut new_state, to_delete) = self
                    .compaction_controller
                    .apply_compaction_result(&current_state, &task, &ids[..], false);
                let to_delete_set: HashSet<usize> = to_delete.iter().copied().collect();
                new_state
                    .sstables
                    .retain(|id, _| !to_delete_set.contains(id));
                for sstable in compacted {
                    new_state.sstables.insert(sstable.sst_id(), sstable);
                }
                *current_state = Arc::new(new_state);
                self.sync_dir()?;
                self.manifest
                    .as_ref()
                    .ok_or(anyhow::anyhow!("no manifest"))?
                    .add_record(&state_lock, ManifestRecord::Compaction(task, ids))?;
                to_delete
            };
        for id in to_delete {
            _ = std::fs::remove_file(self.path_of_sst(id));
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            return self.force_flush_next_imm_memtable();
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
