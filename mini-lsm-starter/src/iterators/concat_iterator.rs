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

use std::sync::Arc;

use anyhow::{Result, bail};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let first_iter = SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[0]))?;
        Ok(Self {
            current: Some(first_iter),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut resp = Self {
            current: None,
            next_sst_idx: 1,
            sstables: Vec::new(),
        };
        for i in 0..sstables.len() {
            let first_iter =
                SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[i]), key)?;
            if !first_iter.is_valid() {
                continue;
            }
            resp.current = Some(first_iter);
            resp.sstables = sstables;
            return Ok(resp);
        }
        Ok(Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key<'b>(&'b self) -> KeySlice<'b> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if !self.current.as_ref().is_some() {
            return false;
        }
        let curr_valid = self.current.as_ref().unwrap().is_valid();
        if curr_valid {
            return curr_valid;
        }
        self.sstables.get(self.next_sst_idx).is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        let curr_valid = self.current.as_ref().unwrap().is_valid();
        if curr_valid {
            self.current.as_mut().unwrap().next()?;
            return Ok(());
        }
        // curr is not valid
        let next_table = Arc::clone(&self.sstables[self.next_sst_idx]);
        let iter = SsTableIterator::create_and_seek_to_first(next_table)?;
        self.next_sst_idx += 1;
        self.current = Some(iter);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
