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

use std::{
    cmp::{self},
    collections::{BinaryHeap, binary_heap::PeekMut},
};

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() || iters.iter().all(|x| !x.is_valid()) {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            current.1.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        while let Some(mut inner) = self.iters.peek_mut() {
            if inner.1.key() == current.1.key() {
                // skip duplicates
                if let e @ Err(_) = inner.1.next() {
                    PeekMut::pop(inner);
                    return e;
                }
                if !inner.1.is_valid() {
                    PeekMut::pop(inner);
                }
            } else {
                break;
            }
        }
        current.1.next()?;
        if !current.1.is_valid() {
            if let Some(val) = self.iters.pop() {
                *current = val;
            }
            return Ok(());
        }
        if let Some(mut inner) = self.iters.peek_mut() {
            if *current < *inner {
                std::mem::swap(&mut *inner, current);
            }
        }
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map_or(0, |x| x.1.num_active_iterators())
    }
}
