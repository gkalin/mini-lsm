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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    size: usize,
    full: bool,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
            full: false,
            size: 2,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.key_len();
        let value_len = value.len();

        if self.full {
            return false;
        }

        // Calculate actual entry size (with compression if applicable)
        let entry_size = if self.first_key.is_empty() {
            // First key: no compression
            2 + 2 + key_len + 8 + 2 + value_len + 2
        } else {
            // Calculate compressed size
            let overlap_len = key
                .key_ref()
                .iter()
                .zip(self.first_key.key_ref().iter())
                .take_while(|(a, b)| a == b)
                .count();
            2 + 2 + (key_len - overlap_len) + 8 + 2 + value_len + 2
        };

        if self.block_size < self.size + entry_size {
            self.full = true;
            if self.first_key.is_empty() {
                // First entry can still fit
                self.first_key = key.to_key_vec();
                // overlap_len = 0 for first key
                self.data.extend_from_slice(&0u16.to_be_bytes());
                // remaining_key_len = full key length
                self.data.extend_from_slice(&(key_len as u16).to_be_bytes());
                // key
                self.data.extend_from_slice(key.key_ref());
                // timestamp
                self.data.extend_from_slice(&key.ts().to_le_bytes());
                // value_len
                self.data
                    .extend_from_slice(&(value_len as u16).to_be_bytes());
                // value
                self.data.extend_from_slice(value);
                self.offsets.push(0);
                self.size += entry_size;
                return true;
            }
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        if self.first_key.is_empty() {
            // First key
            self.first_key = key.to_key_vec();
            // overlap_len = 0
            self.data.extend_from_slice(&0u16.to_be_bytes());
            // remaining_key_len = full key length
            self.data.extend_from_slice(&(key_len as u16).to_be_bytes());
            // key
            self.data.extend_from_slice(key.key_ref());
            // timestamp
            self.data.extend_from_slice(&key.ts().to_le_bytes());
        } else {
            // Subsequent keys - use prefix compression
            let overlap_len = key
                .key_ref()
                .iter()
                .zip(self.first_key.key_ref().iter())
                .take_while(|(a, b)| a == b)
                .count();
            let remaining_key = &key.key_ref()[overlap_len..];

            // overlap_len
            self.data
                .extend_from_slice(&(overlap_len as u16).to_be_bytes());
            // remaining_key_len
            self.data
                .extend_from_slice(&(remaining_key.len() as u16).to_be_bytes());
            // remaining_key
            self.data.extend_from_slice(remaining_key);
            // timestamp
            self.data.extend_from_slice(&key.ts().to_le_bytes());
        }

        self.size += entry_size;
        // value_len
        self.data
            .extend_from_slice(&(value_len as u16).to_be_bytes());
        // value
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
