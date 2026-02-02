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
            if pos + 2 > buffer.len() {
                break;
            }
            let key_len = u16::from_le_bytes([buffer[pos], buffer[pos + 1]]) as usize;
            pos += 2;

            if pos + key_len + 8 + 2 > buffer.len() {
                break;
            }
            let key = Bytes::copy_from_slice(&buffer[pos..pos + key_len]);
            pos += key_len;

            let ts = u64::from_le_bytes(buffer[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let val_len = u16::from_le_bytes([buffer[pos], buffer[pos + 1]]) as usize;
            pos += 2;

            if pos + val_len + 4 > buffer.len() {
                break;
            }
            let val = Bytes::copy_from_slice(&buffer[pos..pos + val_len]);
            pos += val_len;

            let checksum = u32::from_le_bytes([
                buffer[pos],
                buffer[pos + 1],
                buffer[pos + 2],
                buffer[pos + 3],
            ]);
            pos += 4;

            _skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), val);
        }
        return Self::create(_path);
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        let mut guard = self.file.lock();
        let wal = guard.get_mut();

        let key_len = (_key.key_len() as u16).to_le_bytes();
        wal.write_all(&key_len)?;
        wal.write_all(_key.key_ref())?;

        let ts = _key.ts().to_le_bytes();
        wal.write_all(&ts)?;

        let value_len = (_value.len() as u16).to_le_bytes();
        wal.write_all(&value_len)?;
        wal.write_all(_value)?;

        // Calculate checksum over key_len + key + ts + value_len + value
        let mut checksum_data = Vec::new();
        checksum_data.extend_from_slice(&key_len);
        checksum_data.extend_from_slice(_key.key_ref());
        checksum_data.extend_from_slice(&ts);
        checksum_data.extend_from_slice(&value_len);
        checksum_data.extend_from_slice(_value);
        let checksum = crc32fast::hash(&checksum_data);

        wal.write_all(&checksum.to_le_bytes())?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
