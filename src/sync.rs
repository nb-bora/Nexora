//! Lock helpers that ignore poisoning.
//!
//! `std` locks are poisoned when a thread panics while holding them, after which every `lock().unwrap()`
//! panics too: one bug in one request would fail every later request of that tenant. Our critical
//! sections only update plain numbers or maps, so the data is still usable after a panic; carrying on
//! (at worst with one slightly-off counter) is far better than an outage.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait MutexExt<T> {
    fn guard(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn guard(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub trait RwLockExt<T> {
    fn rd(&self) -> RwLockReadGuard<'_, T>;
    fn wr(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn rd(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }
    fn wr(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn poison<T: Send + Sync + 'static>(m: &Arc<Mutex<T>>) {
        let m = m.clone();
        let _ = std::thread::spawn(move || {
            let _held = m.lock().unwrap();
            panic!("simulated bug while holding the lock");
        })
        .join();
    }

    #[test]
    fn mutex_stays_usable_after_a_panic() {
        let m = Arc::new(Mutex::new(7));
        poison(&m);
        assert!(m.lock().is_err(), "precondition: the mutex really is poisoned");
        assert_eq!(*m.guard(), 7);
    }

    #[test]
    fn rwlock_stays_usable_after_a_panic() {
        let l = Arc::new(RwLock::new(1));
        let l2 = l.clone();
        let _ = std::thread::spawn(move || {
            let _held = l2.write().unwrap();
            panic!("simulated bug while holding the lock");
        })
        .join();
        assert!(l.read().is_err(), "precondition: the lock really is poisoned");
        *l.wr() += 1;
        assert_eq!(*l.rd(), 2);
    }
}
