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

use bytes::Bytes;
use std::ops::Bound;

use anyhow::{Result, bail};

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<Bytes>,
    done: bool,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = iter;
        while iter.is_valid() {
            if iter.value().is_empty() {
                // Skip deleted keys and their old versions
                if crate::key::TS_ENABLED {
                    let user_key = iter.key().key_ref().to_vec();
                    while iter.is_valid() && iter.key().key_ref() == user_key.as_slice() {
                        iter.next()?;
                    }
                } else {
                    iter.next()?;
                }
                continue;
            }

            break;
        }
        let prev_key = if iter.is_valid() {
            iter.key().key_ref().to_vec()
        } else {
            Vec::new()
        };
        Ok(Self {
            inner: iter,
            upper_bound,
            done: false,
            prev_key,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.done && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }

        loop {
            self.inner.next()?;
            if !self.inner.is_valid() {
                self.done = true;
                self.prev_key.clear();
                break;
            }

            // Skip old versions of the same user key
            if self.inner.key().key_ref() == self.prev_key.as_slice() {
                continue;
            }

            // Skip deleted keys (and their old versions)
            if self.inner.value().is_empty() {
                if crate::key::TS_ENABLED {
                    let user_key = self.inner.key().key_ref().to_vec();
                    while self.inner.is_valid() && self.inner.key().key_ref() == user_key.as_slice()
                    {
                        self.inner.next()?;
                    }
                    if !self.inner.is_valid() {
                        self.done = true;
                        self.prev_key.clear();
                        break;
                    }
                } else {
                    continue;
                }
            }

            match self.upper_bound.as_ref() {
                Bound::Excluded(bound) => {
                    if self.inner.key().key_ref() >= bound.as_ref() {
                        self.done = true;
                        self.prev_key.clear();
                        break;
                    }
                }
                Bound::Included(bound) => {
                    if self.inner.key().key_ref() > bound.as_ref() {
                        self.done = true;
                        self.prev_key.clear();
                        break;
                    }
                }
                Bound::Unbounded => {}
            }

            self.prev_key = self.inner.key().key_ref().to_vec();
            break;
        }

        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Iterator is not valid");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Iterator is not valid");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Iterator is poisoned");
        } else if !self.is_valid() {
            return Ok(());
        }
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }
    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
