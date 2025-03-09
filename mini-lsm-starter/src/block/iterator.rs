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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block.data[self.value_range.0..self.value_range.1].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let key_len = ((self.block.data[0] as usize) << 8) | (self.block.data[1] as usize);
        self.key = KeyVec::from_vec(self.block.data[2..2 + key_len].to_vec());
        self.idx = 0;
        let value_len = ((self.block.data[key_len + 2] as usize) << 8)
            | (self.block.data[key_len + 3] as usize);
        self.value_range = (4 + key_len, 4 + key_len + value_len);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 == self.block.offsets.len() {
            self.key.clear();
            return;
        }
        self.idx += 1;
        let offset = self.block.offsets[self.idx] as usize;
        let key_len =
            ((self.block.data[offset] as usize) << 8) | (self.block.data[offset + 1] as usize);
        self.key = KeyVec::from_vec(self.block.data[offset + 2..offset + 2 + key_len].to_vec());
        let value_len = ((self.block.data[offset + 2 + key_len] as usize) << 8)
            | (self.block.data[offset + 2 + key_len + 1] as usize);
        self.value_range = (
            offset + 2 + key_len + 2,
            offset + 2 + key_len + 2 + value_len,
        );
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let offset = self.block.offsets[mid] as usize;
            let key_len =
                ((self.block.data[offset] as usize) << 8) | (self.block.data[offset + 1] as usize);
            let mid_key =
                KeySlice::from_slice(self.block.data[offset + 2..offset + 2 + key_len].as_ref());
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left == self.block.offsets.len() {
            self.key.clear();
            return;
        }
        let offset = self.block.offsets[left] as usize;
        let key_len =
            ((self.block.data[offset] as usize) << 8) | (self.block.data[offset + 1] as usize);
        self.key = KeyVec::from_vec(self.block.data[offset + 2..offset + 2 + key_len].to_vec());
        let value_len = ((self.block.data[offset + 2 + key_len] as usize) << 8)
            | (self.block.data[offset + 2 + key_len + 1] as usize);
        self.value_range = (
            offset + 2 + key_len + 2,
            offset + 2 + key_len + 2 + value_len,
        );
    }
}
