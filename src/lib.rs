//! Async & reactive subscription model to keep multiple async tasks / threads partially
//! synchronized.
//!
//! ## Differentiation From Traditional Asnyc Streams
//! **Important:** A subscription is not a clonable `Stream<T>` – versions may be skipped on the
//! subscription side if a subscription doesnt ask for updates anymore or if updates are published
//! to quickly the subscription just retrieves the latest value.
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
//!         let mut subscription = observable.subscribe();
//!
//!         tasks.push(spawn(async move {
//!             let update = subscription.next().await;
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

/// Wraps a value and lets you derive subscriptions to synchronize values between tasks and threads.
///
/// ## Creating Observables
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
/// ## Important
/// **Keep in mind that if you publish multiple versions directly after each other there no guarantees that
/// all subscriptions recieve every change!** But as long as every subscription is constently asking
/// for changes (via `next()`) you are guaranteed that every subscription recieved the latest version.
#[derive(Clone)]
pub struct Observable<T>(Arc<Mutex<Inner<T>>>)
where
    T: Clone;

impl<T> Observable<T>
where
    T: Clone,
{
    /// Create a new observable from any value.
    pub fn new(value: T) -> Self {
        Observable(Arc::new(Mutex::new(Inner::new(value))))
    }

    /// Publish a change to all subscriptions and store it.
    pub fn publish(&mut self, value: T) {
        let mut inner = self.lock();
        inner.version += 1;
        inner.value = value;

        for (_, waker) in &inner.waker {
            waker.wake_by_ref();
        }

        inner.waker.clear();
    }

    /// Create a new subscription for this observable.
    pub fn subscribe(&self) -> Subscription<T> {
        Subscription::from(self)
    }

    /// Creates a clone of the observable value
    pub fn latest(&self) -> T {
        let inner = self.lock();
        inner.value.clone()
    }

    fn lock(&self) -> MutexGuard<Inner<T>> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        }
    }

    #[cfg(test)]
    pub(crate) fn waker_count(&self) -> usize {
        self.0.lock().unwrap().waker.len()
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
            .field("value", &inner.value)
            .field("version", &inner.version)
            .finish()
    }
}

#[derive(Debug)]
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

/// Represents a subscription to an observable value.
///
/// **Important:** A subscription is not guaranteed to fetch every update published,
/// but to always have the latest update at the time you resolve the `next()` future.
///
/// The only ways to retrieve an subscription are calling `subscribe()` on an observable or using
/// `Subscription::from` directly.
///
/// Once subscribed you can use it like this:
/// ```rust
/// # use async_sub::{Observable, Subscription};
/// # async {
/// let mut observable = Observable::new(0);
/// let mut subscription = observable.subscribe();
///
/// observable.publish(1);
/// observable.publish(2);
/// observable.publish(3);
///
/// assert_eq!(subscription.next().await, 3);
/// # };
/// ```
#[derive(Debug)]
pub struct Subscription<T>
where
    T: Clone,
{
    observable: Observable<T>,
    version: u128,
}

impl<T> Subscription<T>
where
    T: Clone,
{
    pub(crate) fn into_inner_mutex(&self) -> MutexGuard<Inner<T>> {
        match self.observable.0.lock() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        }
    }

    /// Wait until a new version of the subscibed observable was published and
    /// return a clone of the new version.
    pub async fn next(&mut self) -> T {
        AwaitSubscriptionUpdate::from(self).await
    }

    /// Creates a clone of latest version of the subscribed value.
    pub fn latest(&self) -> T {
        self.observable.latest()
    }

    /// Skip any potential updates and retrieve the latest version of the
    /// subscribed value.
    ///
    /// ```rust
    /// # use async_sub::{Observable, Subscription};
    /// # async {
    /// let mut observable = Observable::new(0);
    /// let mut subscription = observable.subscribe();
    ///
    /// observable.publish(1);
    /// observable.publish(2);
    /// observable.publish(3);
    ///
    /// assert_eq!(subscription.synchronize(), 3);
    ///
    /// subscription.next().await; // runs forever!
    /// # };
    /// ```
    pub fn synchronize(&mut self) -> T {
        let inner = self.observable.lock();
        self.version = inner.version;
        inner.value.clone()
    }
}

impl<T> From<&Observable<T>> for Subscription<T>
where
    T: Clone,
{
    /// Create a new subscription to an observable value.
    fn from(observable: &Observable<T>) -> Self {
        let version = observable.0.lock().unwrap().version;

        Self {
            observable: observable.clone(),
            version,
        }
    }
}

#[doc(hidden)]
struct AwaitSubscriptionUpdate<'a, T>
where
    T: Clone,
{
    id: u128,
    subscription: &'a mut Subscription<T>,
}

impl<'a, T: Clone> From<&'a mut Subscription<T>> for AwaitSubscriptionUpdate<'a, T> {
    fn from(sub: &'a mut Subscription<T>) -> Self {
        let id = {
            let mut guard = sub.into_inner_mutex();
            let mut inner = guard.deref_mut();
            inner.future_count += 1;
            inner.future_count
        };

        Self {
            id,
            subscription: sub,
        }
    }
}

impl<'a, T> Future for AwaitSubscriptionUpdate<'a, T>
where
    T: Clone,
{
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

impl<'a, T> Drop for AwaitSubscriptionUpdate<'a, T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut guard = self.subscription.into_inner_mutex();
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
        let mut subscription = int.subscribe();

        int.publish(2);
        assert_eq!(subscription.next().await, 2);
        int.publish(3);
        assert_eq!(subscription.next().await, 3);
        int.publish(0);
        assert_eq!(subscription.next().await, 0);
    }

    #[test]
    async fn should_get_notified_sync_multiple() {
        let mut int = Observable::new(1);
        let mut subscription_one = int.subscribe();
        let mut subscription_two = int.subscribe();

        int.publish(2);
        assert_eq!(subscription_one.next().await, 2);
        assert_eq!(subscription_two.next().await, 2);

        int.publish(3);
        assert_eq!(subscription_one.next().await, 3);
        assert_eq!(subscription_two.next().await, 3);

        int.publish(0);
        assert_eq!(subscription_one.next().await, 0);
        assert_eq!(subscription_two.next().await, 0);
    }

    #[test]
    async fn should_skip_unchecked_updates() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);
        assert_eq!(subscription.next().await, 2);
        int.publish(3);
        int.publish(0);
        assert_eq!(subscription.next().await, 0);
    }

    #[test]
    async fn should_wait_for_publisher_task() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        spawn(async move {
            sleep(SLEEP_DURATION).await;
            int.publish(2);
            sleep(SLEEP_DURATION).await;
            int.publish(3);
            sleep(SLEEP_DURATION).await;
            int.publish(0);
        });

        assert_eq!(subscription.next().await, 2);
        assert_eq!(subscription.next().await, 3);
        assert_eq!(subscription.next().await, 0);
    }

    #[test]
    async fn should_skip_versions() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);
        int.publish(3);
        int.publish(0);

        assert_eq!(subscription.next().await, 0);
    }

    #[test]
    async fn should_wait_after_skiped_versions() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);
        int.publish(3);
        int.publish(0);

        assert_eq!(subscription.next().await, 0);
        assert!(timeout(TIMEOUT_DURATION, subscription.next())
            .await
            .is_err());
    }

    #[test]
    async fn should_remove_waker_on_future_drop() {
        let int = Observable::new(1);
        let mut subscription = int.subscribe();

        for _ in 0..100 {
            timeout(Duration::from_millis(10), subscription.next())
                .await
                .ok();

            assert_eq!(int.waker_count(), 0);
        }
    }

    #[test]
    async fn should_wait_forever() {
        let int = Observable::new(1);
        let mut subscription = int.subscribe();

        assert!(timeout(TIMEOUT_DURATION, subscription.next())
            .await
            .is_err());
    }

    #[test]
    async fn should_get_latest_without_loosing_updates() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);

        assert_eq!(subscription.latest(), 2);
        assert_eq!(subscription.latest(), 2);

        assert_eq!(subscription.next().await, 2);
    }

    #[test]
    async fn should_skip_updates_while_synchronizing() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);
        int.publish(3);

        assert_eq!(subscription.synchronize(), 3);

        assert!(timeout(TIMEOUT_DURATION, subscription.next())
            .await
            .is_err());
    }

    #[test]
    async fn should_synchronize_multiple_times() {
        let mut int = Observable::new(1);
        let mut subscription = int.subscribe();

        int.publish(2);
        int.publish(3);

        assert_eq!(subscription.synchronize(), 3);
        assert_eq!(subscription.synchronize(), 3);

        int.publish(4);

        assert_eq!(subscription.synchronize(), 4);

        assert!(timeout(TIMEOUT_DURATION, subscription.next())
            .await
            .is_err());
    }
}
