//! Clone any [`Stream`] `S` where `<S as Stream>::Item: Clone`.
//!
//! ## Usage
//!
//! ```
//! use fork_stream::StreamExt as _;
//! use futures::stream::StreamExt as _;
//!
//! async fn example() {
//!     let source = futures::stream::iter(0..3);
//!
//!     let fork1 = source.fork();
//!
//!     let fork2 = fork1.clone();
//!
//!     assert_eq!(fork1.collect::<Vec<_>>().await, [0, 1, 2]);
//!     assert_eq!(fork2.collect::<Vec<_>>().await, [0, 1, 2]);
//! }
//! ```
//!
//! ### Behavior
//!
//! 1. Polled items from the source stream are stored in a buffer;
//! 2. When forks of the source stream are polled, they either yield clones of the items from the buffer,
//! or poll the source stream for new items;
//! 4. If there are no longer any forks that may read an item (either because the fork is dropped, or because
//! all of the forks have already yielded that item) the item is dropped;
//! 5. Whenever possible, items from the buffer are moved out instead of clonned.
//! 6. When all of the forks are dropped, source stream is dropped.
//!
//! ### `Weak`
//!
//! Any fork can be downgraded to a [`Weak`], which can later be upgraded back, similar to [`std::rc::Rc`] or [`std::sync::Arc`] APIs.
//!
//! This behaves as follows:
//!
//! 1. [`Weak`] does not implement [`Stream`] and cannot be polled without being upgraded first;
//! 2. When a [`Weak`] is upgraded into a [`Forked`], the resulting [`Forked`] is as advanced as the source stream;
//! i.e. it will not yield any items that had been yielded by any other forks prior to the upgrade.
//! 3. If all of the forks had been dropped prior to the upgrade, `Weak::upgrade` returns `None`.
//!
//! `Weak` API is useful when you want to reuse streams that are expensive to intialize,
//! but also want to drop them when they are not needed.
//!
//! ### Differences from `shared_stream`
//!
//! This library implements an API similar to that of [`shared_stream`](https://docs.rs/shared_stream), with a few notable differences:
//!
//! 1. Streams produced by this library are [`Send`] and [`Sync`]. For this reason we have to use synchronisation primitives that
//! support it, which may be less performant, but makes it a more suitable option for async environments.
//! 2. `shared_stream` buffers the items for as long as at least one clone of the source stream exists. This library "garbage collects"
//! the items as soon as possible. This comes at a cost of some extra business logic, which may be less performant, but makes it a more
//! suitable option for situations where streams are supposed to be long-lived, such as servers.
//! 3. This library provides a [`Weak`] API, see above.

use futures::{
    stream::{Fuse, FusedStream},
    Stream, StreamExt as _,
};
use pin_project::pin_project;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

#[cfg(test)]
mod tests;

/// An extension trait that enables [`Stream`]s to be forked.
pub trait StreamExt: Stream {
    /// Turns a steam into a cloneable stream. Polled items are cached and cloned.
    ///
    /// This method consumes the source stream and returns its wrapped version.
    fn fork(self) -> Forked<Self>
    where
        Self: Sized,
        Self::Item: Clone,
    {
        ForkedInner::init(self)
    }
}

impl<S> StreamExt for S where S: Stream {}

/// A wrapper around the source stream `S` that makes it cloneable.
#[must_use = "streams do nothing unless polled"]
pub struct Forked<S: Stream>
where
    S::Item: Clone,
{
    subscriber: Option<BufferSubscriber>,
    inner: Arc<Mutex<Pin<Box<ForkedInner<S>>>>>,
}

impl<S: Stream> Clone for Forked<S>
where
    S::Item: Clone,
{
    fn clone(&self) -> Self {
        ForkedInner::create_fork(
            self.inner.clone(),
            Some(self.subscriber.as_ref().expect("only unset during drop")),
        )
    }
}

impl<S: Stream> Stream for Forked<S>
where
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = Pin::get_mut(self);
        let mut inner = this.inner.lock().unwrap();
        inner.as_mut().poll(
            this.subscriber.as_mut().expect("only unset during drop"),
            cx.waker(),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let inner = self.inner.lock().unwrap();
        inner.size_hint(self.subscriber.as_ref().expect("only unset during drop"))
    }
}

impl<S: Stream> FusedStream for Forked<S>
where
    S::Item: Clone,
{
    fn is_terminated(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_terminated(self.subscriber.as_ref().expect("only unset during drop"))
    }
}

impl<S: Stream> Drop for Forked<S>
where
    S::Item: Clone,
{
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .as_mut()
            .project()
            .buffer
            .dispose_of_subscriber(self.subscriber.take().expect("only unset during drop"));
    }
}

#[pin_project]
struct ForkedInner<S: Stream>
where
    S::Item: Clone,
{
    #[pin]
    source: Fuse<S>,
    buffer: Buffer<S::Item>,
    waiting_for_source: Option<Waker>,
    waiting_for_buffer: VecDeque<Waker>,
}

impl<S: Stream> ForkedInner<S>
where
    S::Item: Clone,
{
    // Initializes the `ForkedInner` and creates the first fork.
    fn init(source: S) -> Forked<S> {
        let inner = Self {
            // Source stream needs to be fused, because the forks will try to poll it once it's exhausted.
            source: source.fuse(),
            buffer: Buffer::new(),
            waiting_for_source: None,
            waiting_for_buffer: VecDeque::new(),
        };
        let arc = Arc::new(Mutex::new(Box::pin(inner)));
        Self::create_fork(arc, None)
    }

    fn poll(
        self: Pin<&mut Self>,
        subscriber: &mut BufferSubscriber,
        waker: &'_ Waker,
    ) -> Poll<Option<S::Item>> {
        let this = self.project();
        // First we look for a relevant item in the buffer;
        let result = if let Some(item) = this.buffer.read(subscriber) {
            // If there is an item from the buffer to yield, do it.
            Poll::Ready(Some(item))
        } else {
            // There may be other forks that are now waiting for a new item from the source;
            // They should now wait for an item from the buffer instead, because we are about to poll the source and put
            // the result into the buffer.
            if let Some(prev_waker) = this.waiting_for_source.replace(waker.clone()) {
                this.waiting_for_buffer.push_back(prev_waker);
            }

            // If there is no item from the buffer to yield, we poll the source stream.
            match Stream::poll_next(this.source, &mut std::task::Context::from_waker(&waker)) {
                Poll::Ready(Some(item)) => {
                    // We populate the item into the buffer then immediately read it from there; The buffer will decide whether
                    // it needs to keep a clone of the item in.
                    this.buffer.push(item);
                    let item = this
                        .buffer
                        .read(subscriber)
                        .expect("the item was just pushed into the buffer");
                    Poll::Ready(Some(item))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        };

        // We are ready to yield something. That can mean one of the two things:
        // a. We just successfully polled source stream and put the item into the buffer. There may be a fork waiting for a new item
        // to appear in the buffer, we need to notify it to poll again.
        // b. We just released a lock over the buffer, and another fork may now read from it.
        //
        // In either case, what we need to do is wake a fork (if any) that has been waiting to read from the buffer.
        if result.is_ready() {
            if let Some(waiting_waker) = this.waiting_for_buffer.pop_front() {
                waiting_waker.wake();
            }
        }

        result
    }

    fn is_terminated(&self, subscriber: &BufferSubscriber) -> bool {
        self.source.is_terminated() && !self.buffer.has_items_for(subscriber)
    }

    fn size_hint(&self, subscriber: &BufferSubscriber) -> (usize, Option<usize>) {
        // FORKED_SIZE_HINT = ITEMS_FROM_BUFFER + ITEMS_FROM_SOURCE
        let source_size_hint = self.source.size_hint();
        let num_buffered_items = self.buffer.num_items_for(subscriber);
        (
            num_buffered_items + source_size_hint.0,
            source_size_hint
                .1
                .map(|source_upper_bound| num_buffered_items + source_upper_bound),
        )
    }

    // Creates a new fork of this `ForkedInner`.
    fn create_fork(
        this: Arc<Mutex<Pin<Box<Self>>>>,
        with_offset_of: Option<&BufferSubscriber>,
    ) -> Forked<S> {
        let subscriber = this
            .lock()
            .unwrap()
            .as_mut()
            .project()
            .buffer
            .create_subscriber(with_offset_of);
        Forked {
            subscriber: Some(subscriber),
            inner: this,
        }
    }
}

impl<S: Stream> Forked<S>
where
    S::Item: Clone,
{
    /// Creates a [`Weak`] from a stream. See module level documentation for more details.
    pub fn downgrade(&self) -> Weak<S> {
        Weak(Arc::downgrade(&self.inner))
    }
}

/// A weak reference to the source stream. Must be upgraded before being polled. See module level documentation for more details.
pub struct Weak<S: Stream>(std::sync::Weak<Mutex<Pin<Box<ForkedInner<S>>>>>)
where
    S::Item: Clone;

impl<S: Stream> Weak<S>
where
    S::Item: Clone,
{
    pub fn upgrade(&self) -> Option<Forked<S>> {
        self.0
            .upgrade()
            .map(|inner| ForkedInner::create_fork(inner, None))
    }
}

// Implementation of a "garbage collected" buffer for stream items.
struct Buffer<T: Clone> {
    // Underlying storage for items
    buf: VecDeque<BufferEntry<T>>,
    // Incremented when the buffer drops items
    buffer_start_offset: usize,
    // Total number of potential readers for the buffer
    num_subscribers: usize,
}

// Represents a reader of the buffer. Can be used to read items from the buffer.
struct BufferSubscriber {
    // Current offset / "caret" that determines what items this particular subscriber has read / is yet to read.
    offset: usize,
}

#[cfg_attr(test, derive(Debug))]
struct BufferEntry<T> {
    // The item itself
    item: T,
    // Number of readers "interested" in this entry, aka number of times this entry may be read before it is removed
    refs: usize,
}

impl<T: Clone> Buffer<T> {
    fn new() -> Self {
        Self {
            buf: VecDeque::new(),
            buffer_start_offset: 0,
            num_subscribers: 0,
        }
    }

    fn create_subscriber(&mut self, with_offset_of: Option<&BufferSubscriber>) -> BufferSubscriber {
        self.num_subscribers += 1;

        let offset = if let Some(with_offset_of) = with_offset_of {
            // If offset is specified, we need to increment reference count of entries.
            for entry in self
                .buf
                .iter_mut()
                .skip(with_offset_of.offset - self.buffer_start_offset)
            {
                entry.refs += 1;
            }
            with_offset_of.offset
        } else {
            // If no offset is specified, the subscriber points to the end of the buffer (i.e. the next item that will be put
            // into the buffer in the future).
            self.buffer_start_offset + self.buf.len()
        };

        BufferSubscriber { offset }
    }

    fn push(&mut self, item: T) {
        self.buf.push_back(BufferEntry {
            item,
            // Any new entry may be read by all currently existing subscribers.
            refs: self.num_subscribers,
        });
    }

    fn read(&mut self, subscriber: &mut BufferSubscriber) -> Option<T> {
        if let Some(entry) = self
            .buf
            .get_mut(subscriber.offset - self.buffer_start_offset)
        {
            // Decrement reference count
            entry.refs -= 1;

            // If reference count is now 0, take item by value; otherwise clone
            let item = if entry.refs == 0 {
                let item = self
                    .buf
                    .pop_front()
                    .expect("there is at least one item in the buffer")
                    .item;
                self.buffer_start_offset += 1;
                item
            } else {
                entry.item.clone()
            };

            // Advance the subscriber
            subscriber.offset += 1;

            Some(item)
        } else {
            None
        }
    }

    fn has_items_for(&self, subscriber: &BufferSubscriber) -> bool {
        subscriber.offset - self.buffer_start_offset < self.buf.len()
    }

    fn num_items_for(&self, subscriber: &BufferSubscriber) -> usize {
        self.buf.len() - (subscriber.offset - self.buffer_start_offset)
    }

    fn dispose_of_subscriber(&mut self, subscriber: BufferSubscriber) {
        self.num_subscribers -= 1;

        // A subscriber is no longer needed; "garbage collect" items that have been waiting to be read only by that subscriber.

        let mut entries_to_remove = 0usize;
        for entry in self
            .buf
            .iter_mut()
            .skip(subscriber.offset - self.buffer_start_offset)
        {
            entry.refs -= 1;
            if entry.refs == 0 {
                entries_to_remove += 1;
            }
        }

        for _ in 0..entries_to_remove {
            self.buf.pop_front();
        }

        self.buffer_start_offset += entries_to_remove;
    }
}
