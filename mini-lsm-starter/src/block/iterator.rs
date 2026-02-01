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
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 == self.block.offsets.len() {
            self.key.clear();
            return;
        }
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if self.block.offsets.is_empty() {
            self.key.clear();
            return;
        }

        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            self.seek_to_idx(mid);
            let mid_key = self.key();

            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        self.seek_to_idx(left);
    }

    /// Helper method to seek to a specific index
    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        let offset = self.block.offsets[idx] as usize;

        // Format: overlap_len (2) | remaining_key_len (2) | remaining_key | timestamp (8) | value_len (2) | value

        // Read overlap_len
        let overlap_len =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;

        // Read remaining_key_len
        let remaining_key_len =
            u16::from_be_bytes([self.block.data[offset + 2], self.block.data[offset + 3]]) as usize;

        // Read remaining_key
        let remaining_key = &self.block.data[offset + 4..offset + 4 + remaining_key_len];

        // Read timestamp
        let ts_bytes: [u8; 8] = self.block.data
            [offset + 4 + remaining_key_len..offset + 4 + remaining_key_len + 8]
            .try_into()
            .unwrap();
        let timestamp = u64::from_le_bytes(ts_bytes);

        // Reconstruct full key
        if idx == 0 {
            // First key: overlap_len is 0, remaining_key is the full key
            self.key = KeyVec::from_vec_with_ts(remaining_key.to_vec(), timestamp);
            self.first_key = self.key.clone();
        } else {
            // Ensure first_key is initialized
            if self.first_key.is_empty() {
                // Initialize by decoding first key
                let first_offset = self.block.offsets[0] as usize;
                let first_remaining_key_len = u16::from_be_bytes([
                    self.block.data[first_offset + 2],
                    self.block.data[first_offset + 3],
                ]) as usize;
                let first_remaining_key =
                    &self.block.data[first_offset + 4..first_offset + 4 + first_remaining_key_len];
                let first_ts_bytes: [u8; 8] =
                    self.block.data[first_offset + 4 + first_remaining_key_len
                        ..first_offset + 4 + first_remaining_key_len + 8]
                        .try_into()
                        .unwrap();
                let first_ts = u64::from_le_bytes(first_ts_bytes);
                self.first_key = KeyVec::from_vec_with_ts(first_remaining_key.to_vec(), first_ts);
            }

            // Build full key from overlap + remaining
            let mut full_key = self.first_key.key_ref()[..overlap_len].to_vec();
            full_key.extend_from_slice(remaining_key);
            self.key = KeyVec::from_vec_with_ts(full_key, timestamp);
        }

        // Read value
        let value_offset = offset + 4 + remaining_key_len + 8;
        let value_len = u16::from_be_bytes([
            self.block.data[value_offset],
            self.block.data[value_offset + 1],
        ]) as usize;
        self.value_range = (value_offset + 2, value_offset + 2 + value_len);
    }
}
