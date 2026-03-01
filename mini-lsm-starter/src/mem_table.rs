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

use std::fmt;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::table::{SsTable, SsTableBuilder};
use crate::wal::Wal;
use crossbeam_skiplist::map::Entry;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `KeyBytes` from a bound of `KeySlice`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.to_key_vec().into_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.to_key_vec().into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(_path)?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let skipmap = SkipMap::new();
        let wal = Wal::recover(_path, &skipmap)?;
        Ok(MemTable {
            map: Arc::new(skipmap),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_slice = KeySlice::for_testing_from_slice_no_ts(key);
        self.put(key_slice, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        // This function is only used in week 1 tests, so during the week 3 key-ts refactor, you do
        // not need to consider the bound exclude/include logic. Simply provide `DEFAULT_TS` as the
        // timestamp for the key-ts pair.
        let lower_bound = match lower {
            Bound::Included(key) => Bound::Included(KeySlice::for_testing_from_slice_no_ts(key)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(key)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper_bound = match upper {
            Bound::Included(key) => Bound::Included(KeySlice::for_testing_from_slice_no_ts(key)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(key)),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.scan(lower_bound, upper_bound)
    }

    /// Get a value by key.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        // Use transmute to convert &[u8] to &'static [u8] temporarily for lookup
        // This is safe because we're only using it for the lookup and not storing it
        let mapkey = Bytes::from_static(unsafe { std::mem::transmute(_key) });
        self.map
            .get(&KeyBytes::for_testing_from_bytes_no_ts(mapkey))
            .map(|v| v.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.put_batch(_data)?;
        }
        let mut total_size = 0;
        for (key, value) in _data {
            let mapkey = key.to_key_vec().into_key_bytes();
            self.map.insert(mapkey, Bytes::copy_from_slice(value));
            total_size += key.raw_len() + value.len();
        }
        self.approximate_size
            .fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<KeySlice>, _upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_bound(_lower), map_bound(_upper))),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        // safe to unwrap
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            _builder.add(entry.key().as_key_slice(), entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    pub fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|e| (e.key().clone(), e.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let iter = self.with_iter_mut(|iter| iter.next());
        let item = MemTableIterator::entry_to_item(iter);
        self.with_mut(|s| *s.item = item);
        Ok(())
    }
}
