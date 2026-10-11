use common::{PlatformError, Result};
use std::collections::HashMap;

/// Values JavaScript names by a `u32` handle. Handle 0 means "none" and
/// never names a value. Handles count up and wrap, skipping any still in
/// use, so a closed handle is not handed out again until the counter comes
/// back around.
///
/// The op state keeps one value per type, so each table holds a value type
/// of its own.
pub(crate) struct HandleTable<T> {
    next: u32,
    values: HashMap<u32, T>,
}

impl<T> Default for HandleTable<T> {
    fn default() -> Self {
        Self {
            next: 0,
            values: HashMap::new(),
        }
    }
}

impl<T> HandleTable<T> {
    pub(crate) fn insert(&mut self, value: T) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next != 0 && !self.values.contains_key(&self.next) {
                self.values.insert(self.next, value);
                return self.next;
            }
        }
    }

    /// Puts back a value an op took out of `handle` while it awaited.
    pub(crate) fn restore(&mut self, handle: u32, value: T) {
        self.values.insert(handle, value);
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&T> {
        self.values.get(&handle)
    }

    pub(crate) fn get_mut(&mut self, handle: u32) -> Option<&mut T> {
        self.values.get_mut(&handle)
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<T> {
        self.values.remove(&handle)
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        self.values.retain(|_, value| keep(value));
    }
}

/// A value in an [`OwnedHandleTable`], which belongs to one request.
pub(crate) trait OwnedHandle {
    /// The table's name in its limit error, such as "memory read handles".
    const NAME: &'static str;
    /// What the limit counts, after the number in its error.
    const UNIT: &'static str = "active handles";

    /// The request context handle the value belongs to.
    fn owner(&self) -> u32;

    /// What the value counts against its owner's limit: 1 where the limit
    /// is a number of handles, a size where it is a number of bytes.
    fn cost(&self) -> usize {
        1
    }
}

/// A [`HandleTable`] of values that requests own. Each request holds at most
/// a limit's worth at once, and a finished request's values go together.
pub(crate) struct OwnedHandleTable<T> {
    handles: HandleTable<T>,
    usage: HashMap<u32, usize>,
}

impl<T> Default for OwnedHandleTable<T> {
    fn default() -> Self {
        Self {
            handles: HandleTable::default(),
            usage: HashMap::new(),
        }
    }
}

impl<T: OwnedHandle> OwnedHandleTable<T> {
    /// Stores `value`, unless that takes its owner past `limit`.
    pub(crate) fn insert(&mut self, value: T, limit: usize) -> Result<u32> {
        let owner = value.owner();
        let usage = self.usage(owner).saturating_add(value.cost());
        if usage > limit {
            return Err(PlatformError::bad_request(format!(
                "{} exceeded {limit} {}",
                T::NAME,
                T::UNIT
            )));
        }
        let handle = self.handles.insert(value);
        self.usage.insert(owner, usage);
        Ok(handle)
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&T> {
        self.handles.get(handle)
    }

    /// The value behind `handle`, to change in ways that keep its owner and
    /// cost.
    pub(crate) fn get_mut(&mut self, handle: u32) -> Option<&mut T> {
        self.handles.get_mut(handle)
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<T> {
        let value = self.handles.remove(handle)?;
        let owner = value.owner();
        if let Some(usage) = self.usage.get_mut(&owner) {
            *usage = usage.saturating_sub(value.cost());
            if *usage == 0 {
                self.usage.remove(&owner);
            }
        }
        Some(value)
    }

    /// Drops every value `owner` holds.
    pub(crate) fn clear_owner(&mut self, owner: u32) {
        self.handles.retain(|value| value.owner() != owner);
        self.usage.remove(&owner);
    }

    /// What `owner`'s values count against its limit.
    pub(crate) fn usage(&self, owner: u32) -> usize {
        self.usage.get(&owner).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_skip_zero_and_live_handles_when_they_wrap() {
        let mut table = HandleTable {
            next: u32::MAX - 1,
            values: HashMap::new(),
        };
        let last = table.insert("last");
        assert_eq!(last, u32::MAX);
        let first = table.insert("first");
        assert_eq!(first, 1);
        table.next = u32::MAX - 1;
        assert_eq!(table.insert("skips both"), 2);
        assert_eq!(table.get(0), None);
        assert_eq!(table.remove(last), Some("last"));
        assert_eq!(table.get(last), None);
    }
}
