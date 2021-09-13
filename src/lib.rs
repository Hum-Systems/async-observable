//! Async & reactive synchronization model to keep multiple async tasks / threads partially
//! synchronized.
//!
//! ## Differentiation From Traditional Asnyc Streams
//! **Important:** An observable is not a clonable `Stream<T>` – versions may be skipped on the
//! receiving side, if it doesnt ask for updates anymore or if updates are published to quickly the
//! receiving observable just retrieves the latest value.
//!
//! This is a powerful concept since it allows you to just skip the versions which are outdated by
//! a newer version anyway and hence gain some performance advantage through the lazyness implied
//! by this concept. Although the performance aspect is probably unimportant in most usecases it
//! allows you to write simpler code since you dont need to take your position in the stream into
//! account.
//!
//! ## Examples
//! ### Sharing A Counter Between Tasks
//! ```rust
//! use async_std::task::spawn;
//! use async_sub::Observable;
//!
//! #[async_std::main]
//! async fn main() {
//!     let mut observable = Observable::new(0);
//!     let mut tasks = vec![];
//!
//!     for i in 0..10 {
//!         let mut fork = observable.fork();
//!
//!         tasks.push(spawn(async move {
//!             let update = fork.next().await;
//!
//!             println!(
//!                 "Task {} was notified about updated observable {}",
//!                 i, update
//!             );
//!         }));
//!     }
//!
//!     observable.publish(1);
//!
//!     for t in tasks {
//!         t.await
//!     }
//! }
//! ```
use futures::Future;
use std::{
    collections::HashMap,
    fmt,
    ops::DerefMut,
    sync::{Arc, Mutex, MutexGuard},
    task::{Poll, Waker},
};

/// Wraps a value and lets you fork the value to synchronize it between tasks and threads.
///
/// ## Creating New Observables
/// There are several ways to create a new observable, altough using the `new` function should be
/// the preferred way.
///
/// ```rust
/// # use async_sub::Observable;
/// let mut using_new = Observable::new(0);
/// let mut using_from = Observable::from(0);
/// let mut using_into: Observable<u8> = 0.into();
/// ```
///
/// ## Publishing New Values
/// Publishing a new version is done by a single call to the `publish()` method.
///
/// ```rust
/// # use async_sub::Observable;
/// # let mut observable = Observable::new(0);
/// observable.publish(1);
/// observable.publish(2);
/// observable.publish(3);
/// ```
///
/// ## Receiving Updates
///
/// ### Important
/// **Keep in mind that if you publish multiple versions directly after each other there no
/// guarantees that all forked observables will receive every change!** But as long as every
/// observable is constently asking for changes (via `next()`) you are guaranteed that every
/// observable received the latest version.
///
/// ```rust
/// # use async_sub::Observable;
/// # async {
/// let mut observable = Observable::new(0);
/// let mut fork = observable.fork();
///
/// observable.publish(1);
/// observable.publish(2);
/// observable.publish(3);
///
/// assert_eq!(fork.next().await, 3);
/// # };
/// ```
#[derive(Clone)]
pub struct Observable<T>
where
    T: Clone,
{
    inner: Arc<Mutex<Inner<T>>>,
    version: u128,
}

impl<T> Observable<T>
where
    T: Clone,
{
    /// Create a new observable from any value.
    pub fn new(value: T) -> Self {
        Observable {
            inner: Arc::new(Mutex::new(Inner::new(value))),
            version: 0,
        }
    }

    /// Publish a change all observables and store it.
    pub fn publish(&mut self, value: T) {
        let mut inner = self.lock();
        inner.version += 1;
        inner.value = value;

        for (_, waker) in &inner.waker {
            waker.wake_by_ref();
        }

        inner.waker.clear();
    }

    /// Create a new observable from this observable.
    pub fn fork(&self) -> Observable<T> {
        self.clone()
    }

    /// Create a new observable from this observable and reset it to the initial state.
    pub fn fork_and_reset(&self) -> Observable<T> {
        Self {
            inner: self.inner.clone(),
            version: 0,
        }
    }

    /// Creates a clone of latest version of the observable value.
    pub fn latest(&self) -> T {
        let inner = self.lock();
        inner.value.clone()
    }

    /// Wait until a new version of the observable was published and return a
    /// clone of the new version.
    pub async fn next(&mut self) -> T {
        AwaitObservableUpdate::from(self).await
    }

    /// Skip any potential updates and retrieve the latest version of the
    /// observed value.
    ///
    /// ```rust
    /// # use async_sub::Observable;
    /// # async {
    /// let mut observable = Observable::new(0);
    /// let mut fork = observable.fork();
    ///
    /// observable.publish(1);
    /// observable.publish(2);
    /// observable.publish(3);
    ///
    /// assert_eq!(fork.synchronize(), 3);
    ///
    /// fork.next().await; // runs forever!
    /// # };
    /// ```
    pub fn synchronize(&mut self) -> T {
        let (value, version) = {
            let inner = self.lock();
            (inner.value.clone(), inner.version)
        };

        self.version = version;
        value
    }

    pub(crate) fn lock(&self) -> MutexGuard<Inner<T>> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        }
    }

    #[cfg(test)]
    pub(crate) fn waker_count(&self) -> usize {
        self.inner.lock().unwrap().waker.len()
    }
}

impl<T> From<T> for Observable<T>
where
    T: Clone,
{
    /// Create a new observable from any value. Same as calling `new`.
    fn from(value: T) -> Self {
        Observable::new(value)
    }
}

impl<T> fmt::Debug for Observable<T>
where
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.lock();

        f.debug_struct("Observable")
            .field("inner", &inner)
            .field("version", &self.version)
            .finish()
    }
}

struct Inner<T>
where
    T: Clone,
{
    version: u128,
    future_count: u128,
    value: T,
    waker: HashMap<u128, Waker>,
}

impl<T> Inner<T>
where
    T: Clone,
{
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

impl<T> fmt::Debug for Inner<T>
where
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("value", &self.value)
            .field("version", &self.version)
            .finish()
    }
}

#[doc(hidden)]
struct AwaitObservableUpdate<'a, T>
where
    T: Clone,
{
    id: u128,
    observable: &'a mut Observable<T>,
}

impl<'a, T: Clone> From<&'a mut Observable<T>> for AwaitObservableUpdate<'a, T> {
    fn from(obs: &'a mut Observable<T>) -> Self {
        let id = {
            let mut guard = obs.lock();
            let mut inner = guard.deref_mut();
            inner.future_count += 1;
            inner.future_count
        };

        Self {
            id,
            observable: obs,
        }
    }
}

impl<'a, T> Future for AwaitObservableUpdate<'a, T>
where
    T: Clone,
{
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let mut guard = self.observable.lock();
        let inner = guard.deref_mut();

        if self.observable.version == inner.version {
            inner.add_waker(self.id, cx.waker().clone());
            Poll::Pending
        } else {
            inner.remove_waker(self.id);
            let (version, value) = (inner.version, inner.value.clone());

            drop(guard);

            self.observable.version = version;
            Poll::Ready(value)
        }
    }
}

impl<'a, T> Drop for AwaitObservableUpdate<'a, T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut guard = self.observable.lock();
        let inner = guard.deref_mut();
        inner.remove_waker(self.id);
    }
}

#[cfg(test)]
mod test {
    use super::Observable;
    use async_std::future::timeout;
    use async_std::task::{sleep, spawn};
    use async_std::test;
    use std::time::Duration;

    const SLEEP_DURATION: Duration = Duration::from_millis(25);
    const TIMEOUT_DURATION: Duration = Duration::from_millis(500);

    #[test]
    async fn should_get_notified_sync() {
        let mut int = Observable::new(1);
        let mut other = int.fork();

        int.publish(2);
        assert_eq!(other.next().await, 2);
        int.publish(3);
        assert_eq!(other.next().await, 3);
        int.publish(0);
        assert_eq!(other.next().await, 0);
    }

    #[test]
    async fn should_get_notified_sync_multiple() {
        let mut int = Observable::new(1);
        let mut fork_one = int.fork();
        let mut fork_two = int.fork();

        int.publish(2);
        assert_eq!(fork_one.next().await, 2);
        assert_eq!(fork_two.next().await, 2);

        int.publish(3);
        assert_eq!(fork_one.next().await, 3);
        assert_eq!(fork_two.next().await, 3);

        int.publish(0);
        assert_eq!(fork_one.next().await, 0);
        assert_eq!(fork_two.next().await, 0);
    }

    #[test]
    async fn should_skip_unchecked_updates() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);
        assert_eq!(fork.next().await, 2);
        int.publish(3);
        int.publish(0);
        assert_eq!(fork.next().await, 0);
    }

    #[test]
    async fn should_wait_for_publisher_task() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        spawn(async move {
            sleep(SLEEP_DURATION).await;
            int.publish(2);
            sleep(SLEEP_DURATION).await;
            int.publish(3);
            sleep(SLEEP_DURATION).await;
            int.publish(0);
        });

        assert_eq!(fork.next().await, 2);
        assert_eq!(fork.next().await, 3);
        assert_eq!(fork.next().await, 0);
    }

    #[test]
    async fn should_skip_versions() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);
        int.publish(3);
        int.publish(0);

        assert_eq!(fork.next().await, 0);
    }

    #[test]
    async fn should_wait_after_skiped_versions() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);
        int.publish(3);
        int.publish(0);

        assert_eq!(fork.next().await, 0);
        assert!(timeout(TIMEOUT_DURATION, fork.next()).await.is_err());
    }

    #[test]
    async fn should_remove_waker_on_future_drop() {
        let int = Observable::new(1);
        let mut fork = int.fork();

        for _ in 0..100 {
            timeout(Duration::from_millis(10), fork.next()).await.ok();

            assert_eq!(int.waker_count(), 0);
        }
    }

    #[test]
    async fn should_wait_forever() {
        let int = Observable::new(1);
        let mut fork = int.fork();

        assert!(timeout(TIMEOUT_DURATION, fork.next()).await.is_err());
    }

    #[test]
    async fn should_get_latest_without_loosing_updates() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);

        assert_eq!(fork.latest(), 2);
        assert_eq!(fork.latest(), 2);

        assert_eq!(fork.next().await, 2);
    }

    #[test]
    async fn should_skip_updates_while_synchronizing() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);
        int.publish(3);

        assert_eq!(fork.synchronize(), 3);

        assert!(timeout(TIMEOUT_DURATION, fork.next()).await.is_err());
    }

    #[test]
    async fn should_synchronize_multiple_times() {
        let mut int = Observable::new(1);
        let mut fork = int.fork();

        int.publish(2);
        int.publish(3);

        assert_eq!(fork.synchronize(), 3);
        assert_eq!(fork.synchronize(), 3);

        int.publish(4);

        assert_eq!(fork.synchronize(), 4);

        assert!(timeout(TIMEOUT_DURATION, fork.next()).await.is_err());
    }
}
