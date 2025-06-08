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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
            self.meta.push(BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes(self.first_key.clone().into()),
                last_key: KeyBytes::from_bytes(self.first_key.clone().into()),
            });
        }
        if !self.builder.add(key, value) {
            // done with current block
            let new_builder = BlockBuilder::new(self.block_size);
            let old_builder = std::mem::replace(&mut self.builder, new_builder);
            let old_block = old_builder.build();
            self.data.extend(old_block.encode());
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(key.raw_ref().to_vec().into()),
                last_key: KeyBytes::from_bytes(key.raw_ref().to_vec().into()),
            });
        }
        self.meta.last_mut().unwrap().last_key = KeyBytes::from_bytes(self.last_key.clone().into());
        let _ = self.builder.add(key, value);
        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let block = self.builder.build();
        let meta_block_offset = self.data.len();
        let file = FileObject::create(path.as_ref(), self.data)?;
        let sstable = SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_block_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(self.first_key.into()),
            last_key: KeyBytes::from_bytes(self.last_key.into()),
            bloom: None,
            max_ts: 0,
        };
        Ok(sstable)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
