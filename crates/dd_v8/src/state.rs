use futures_util::task::AtomicWaker;
use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::Arc;

/// Per-runtime host state, keyed by type. Ops read and write it; the host
/// fills it before running any script.
pub struct OpState {
    values: HashMap<TypeId, Box<dyn Any>>,
    /// Wakes the runtime's event loop. Anything that makes progress possible
    /// outside an op future (a queue the loop drains, for instance) wakes it.
    pub waker: Arc<AtomicWaker>,
}

impl OpState {
    pub(crate) fn new(waker: Arc<AtomicWaker>) -> Self {
        Self {
            values: HashMap::new(),
            waker,
        }
    }

    pub fn put<T: 'static>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn has<T: 'static>(&self) -> bool {
        self.values.contains_key(&TypeId::of::<T>())
    }

    pub fn try_borrow<T: 'static>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }

    pub fn try_borrow_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.values
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut())
    }

    // Named after `RefCell` and Deno's `OpState`, which the runtime's ops
    // were written against.
    #[allow(clippy::should_implement_trait)]
    pub fn borrow<T: 'static>(&self) -> &T {
        self.try_borrow()
            .unwrap_or_else(|| panic!("op state has no {}", type_name::<T>()))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn borrow_mut<T: 'static>(&mut self) -> &mut T {
        self.try_borrow_mut()
            .unwrap_or_else(|| panic!("op state has no {}", type_name::<T>()))
    }

    pub fn try_take<T: 'static>(&mut self) -> Option<T> {
        self.values
            .remove(&TypeId::of::<T>())
            .and_then(|value| value.downcast().ok())
            .map(|value| *value)
    }

    pub fn take<T: 'static>(&mut self) -> T {
        self.try_take()
            .unwrap_or_else(|| panic!("op state has no {}", type_name::<T>()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_keyed_by_type() {
        let mut state = OpState::new(Arc::default());
        state.put(7u32);
        state.put(String::from("seven"));
        *state.borrow_mut::<u32>() += 1;
        assert_eq!(*state.borrow::<u32>(), 8);
        assert_eq!(state.borrow::<String>(), "seven");
        assert!(state.try_borrow::<u64>().is_none());
        assert_eq!(state.take::<u32>(), 8);
        assert!(!state.has::<u32>());
    }
}
