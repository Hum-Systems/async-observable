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

impl<T: Clone> Subscribtion<T> {
    // TODO: Can we ever have a poisoned mutex? Do we need to recover?
    pub(crate) fn into_inner_mutex(&self) -> MutexGuard<Inner<T>> {
        self.observable.0.lock().unwrap()
    }

    pub fn next(&mut self) -> AwaitSubscriptionUpdate<'_, T> {
        let id = {
            let mut guard = self.into_inner_mutex();
            let mut inner = guard.deref_mut();
            inner.future_count += 1;
            inner.future_count
        };

        AwaitSubscriptionUpdate {
            id,
            subscription: self,
        }
    }
}

pub struct AwaitSubscriptionUpdate<'a, T: Clone> {
    id: u128,
    subscription: &'a mut Subscribtion<T>,
}

impl<'a, T: Clone> Future for AwaitSubscriptionUpdate<'a, T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let mut guard = self.subscription.into_inner_mutex();
        let inner = guard.deref_mut();

        if self.subscription.version == inner.version {
            inner.add_waker(self.id, cx.waker().clone());
            Poll::Pending
        } else {
            inner.remove_waker(self.id);
            let (version, value) = (inner.version, inner.value.clone());

            drop(guard);

            self.subscription.version = version;
            Poll::Ready(value)
        }
    }
}

impl<'a, T: Clone> Drop for AwaitSubscriptionUpdate<'a, T> {
    fn drop(&mut self) {
        let mut guard = self.subscription.into_inner_mutex();
        let inner = guard.deref_mut();
        inner.remove_waker(self.id);
    }
}
