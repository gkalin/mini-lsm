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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // L0 trigger
        let mut task = SimpleLeveledCompactionTask::default();
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            task.upper_level_sst_ids = _snapshot.l0_sstables.clone();
            task.lower_level_sst_ids = _snapshot.levels[0].1.clone();
            task.lower_level = 0;
            task.is_lower_level_bottom_level = task.lower_level == _snapshot.levels.len() - 1;
            return Some(task);
        }
        let level_sizes: Vec<usize> = _snapshot.levels.iter().map(|level| level.1.len()).collect();
        for (lower, window) in level_sizes.windows(2).enumerate() {
            let (curr, next) = (window[0], window[1]);
            if next * 100 >= self.options.size_ratio_percent * curr {
                continue;
            }
            task.upper_level = Some(lower);
            task.lower_level = lower + 1;
            task.upper_level_sst_ids = _snapshot.levels[lower].1.clone();
            task.lower_level_sst_ids = _snapshot.levels[task.lower_level].1.clone();
            task.is_lower_level_bottom_level = task.lower_level == _snapshot.levels.len() - 1;
            return Some(task);
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let to_delete: HashSet<usize> = _task
            .upper_level_sst_ids
            .iter()
            .chain(_task.lower_level_sst_ids.iter())
            .copied()
            .collect();
        let mut new_state = _snapshot.clone();
        if let Some(upper) = _task.upper_level {
            new_state.levels[upper]
                .1
                .retain(|id| !to_delete.contains(id));
        } else {
            new_state.l0_sstables.retain(|id| !to_delete.contains(id));
        }

        new_state.levels[_task.lower_level]
            .1
            .retain(|id| !to_delete.contains(id));
        new_state.levels[_task.lower_level]
            .1
            .extend_from_slice(_output);
        (new_state, to_delete.into_iter().collect())
    }
}
