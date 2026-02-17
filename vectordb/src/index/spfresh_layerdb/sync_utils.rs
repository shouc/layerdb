use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub(crate) fn lock_mutex<T>(m: &Arc<Mutex<T>>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn lock_read<T>(m: &Arc<RwLock<T>>) -> RwLockReadGuard<'_, T> {
    match m.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn lock_write<T>(m: &Arc<RwLock<T>>) -> RwLockWriteGuard<'_, T> {
    match m.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
