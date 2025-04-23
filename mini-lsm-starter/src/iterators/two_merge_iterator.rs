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

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    which: Which,
    done: bool,
}

#[derive(Debug, Clone, Copy)]
enum Which {
    First,
    Second,
    Default,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut default = Self {
            a,
            b,
            which: Which::Default,
            done: false,
        };
        default.choose_next()?;
        Ok(default)
    }
    fn choose_next(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        match self.which {
            Which::First => {
                self.a.next()?;
            }
            Which::Second => {
                self.b.next()?;
            }
            Which::Default => {}
        }
        if !self.a.is_valid() && !self.b.is_valid() {
            self.done = true;
            return Ok(());
        } else if !self.a.is_valid() {
            self.which = Which::Second;
            return Ok(());
        } else if !self.b.is_valid() {
            self.which = Which::First;
            return Ok(());
        }
        let cmp = self.a.key().cmp(&self.b.key());
        match cmp {
            std::cmp::Ordering::Less => {
                self.which = Which::First;
            }
            std::cmp::Ordering::Greater => {
                self.which = Which::Second;
            }
            std::cmp::Ordering::Equal => {
                self.which = Which::First;
            }
        }
        match self.which {
            Which::First => {
                while self.b.is_valid() && self.a.key() == self.b.key() {
                    self.b.next()?;
                }
            }
            Which::Second => {
                while self.a.is_valid() && self.a.key() == self.b.key() {
                    self.a.next()?;
                }
            }
            Which::Default => {
                unreachable!();
            }
        }
        Ok(())
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.which {
            Which::First => self.a.key(),
            Which::Second => self.b.key(),
            Which::Default => unreachable!(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.which {
            Which::First => self.a.value(),
            Which::Second => self.b.value(),
            Which::Default => unreachable!(),
        }
    }

    fn is_valid(&self) -> bool {
        !self.done
    }

    fn next(&mut self) -> Result<()> {
        self.choose_next()
    }
}
