use futures::Future;
use std::{
    collections::HashMap,
    ops::DerefMut,
    sync::{Arc, Mutex, MutexGuard},
    task::{Poll, Waker},
};

// u128 in observable mit für jeden future,

#[derive(Clone, Debug)]
pub struct Observable<T: Clone>(Arc<Mutex<Inner<T>>>);

impl<T: Clone> Observable<T> {
    pub fn new(value: T) -> Self {
        Observable(Arc::new(Mutex::new(Inner::new(value))))
    }

    pub fn publish(&mut self, value: T) {
        let mut inner = self.0.lock().unwrap();
        inner.version += 1;
        inner.value = value;

        for (_, waker) in &inner.waker {
            waker.wake_by_ref();
        }

        inner.waker.clear();
    }

    pub fn subscribe(&self) -> Subscribtion<T> {
        let version = self.0.lock().unwrap().version;

        Subscribtion {
            observable: self.clone(),
            version,
        }
    }

    #[cfg(test)]
    pub fn waker_count(&self) -> usize {
        self.0.lock().unwrap().waker.len()
    }
}

#[derive(Debug)]
struct Inner<T: Clone> {
    version: u128,
    future_count: u128,
    value: T,
    waker: HashMap<u128, Waker>,
}

impl<T: Clone> Inner<T> {
    pub fn new(value: T) -> Self {
        Self {
            version: 0,
            future_count: 0,
            value,
            waker: HashMap::new(),
        }
    }

    pub fn add_waker(&mut self, id: u128, waker: Waker) {
        self.waker.insert(id, waker);
    }

    pub fn remove_waker(&mut self, id: u128) {
        self.waker.remove(&id);
    }
}

pub struct Subscribtion<T: Clone> {
    observable: Observable<T>,
    version: u128,
}
