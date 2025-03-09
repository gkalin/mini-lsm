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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut combined = Vec::from(self.data.as_slice());
        for offset in &self.offsets {
            combined.push((offset >> 8) as u8);
            combined.push((offset & 0xFF) as u8);
        }
        combined.push((self.offsets.len() >> 8) as u8);
        combined.push((self.offsets.len() & 0xFF) as u8);
        Bytes::from(combined)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_offsets = (data[data.len() - 2] as usize * 256) | (data[data.len() - 1] as usize);
        let offsets_start = data.len() - 2 - num_offsets * 2;
        let mut block = Block {
            data: data[0..offsets_start].to_vec(),
            offsets: Vec::new(),
        };

        for i in 0..num_offsets {
            let idx = offsets_start + i * 2;
            let offset = ((data[idx] as u16) << 8) | (data[idx + 1] as u16);
            block.offsets.push(offset);
        }
        block
    }
}
