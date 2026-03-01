// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .create(true)
            .append(true)
            .open(_path.as_ref())?;
        let writer = BufWriter::new(file);
        Ok(Wal {
            file: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(_path.as_ref())?;
        let mut buffer: Vec<u8> = Vec::new();
        let mut reader = BufReader::new(file);
        reader.read_to_end(&mut buffer)?;
        let mut pos = 0;
        while pos < buffer.len() {
            // Read header: batch_size (u32)
            if pos + 4 > buffer.len() {
                break;
            }
            let batch_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + batch_size + 4 > buffer.len() {
                break;
            }

            let body = &buffer[pos..pos + batch_size];
            let checksum = u32::from_le_bytes(
                buffer[pos + batch_size..pos + batch_size + 4]
                    .try_into()
                    .unwrap(),
            );

            if crc32fast::hash(body) != checksum {
                break;
            }

            // Parse body
            let mut body_pos = 0;
            while body_pos < body.len() {
                let key_len =
                    u16::from_le_bytes(body[body_pos..body_pos + 2].try_into().unwrap()) as usize;
                body_pos += 2;

                let key = Bytes::copy_from_slice(&body[body_pos..body_pos + key_len]);
                body_pos += key_len;

                let ts = u64::from_le_bytes(body[body_pos..body_pos + 8].try_into().unwrap());
                body_pos += 8;

                let val_len =
                    u16::from_le_bytes(body[body_pos..body_pos + 2].try_into().unwrap()) as usize;
                body_pos += 2;

                let val = Bytes::copy_from_slice(&body[body_pos..body_pos + val_len]);
                body_pos += val_len;

                _skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), val);
            }

            pos += batch_size + 4;
        }
        Self::create(_path)
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut guard = self.file.lock();
        let wal = guard.get_mut();

        // Build body
        let mut body = Vec::new();
        for (key, value) in _data {
            body.extend_from_slice(&(key.key_len() as u16).to_le_bytes());
            body.extend_from_slice(key.key_ref());
            body.extend_from_slice(&key.ts().to_le_bytes());
            body.extend_from_slice(&(value.len() as u16).to_le_bytes());
            body.extend_from_slice(value);
        }

        // Write header (batch_size = body length)
        wal.write_all(&(body.len() as u32).to_le_bytes())?;
        // Write body
        wal.write_all(&body)?;
        // Write footer (checksum of body)
        let checksum = crc32fast::hash(&body);
        wal.write_all(&checksum.to_le_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
