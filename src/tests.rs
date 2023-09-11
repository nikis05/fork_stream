use super::{Forked, StreamExt as _, Weak};
use futures::{
    stream::{self, FusedStream},
    Stream, StreamExt as _,
};
use insta::assert_debug_snapshot;
use std::task::Poll;

fn create_test_stream() -> impl Stream<Item = i32> {
    let mut current_poll = -1;
    stream::poll_fn(move |ctx| {
        current_poll += 1;
        if current_poll == 8 {
            Poll::Ready(None)
        } else if current_poll % 2 == 0 {
            Poll::Ready(Some(current_poll / 2))
        } else {
            let waker = ctx.waker().clone();
            tokio::task::spawn(async move { waker.wake() });
            Poll::Pending
        }
    })
}

/// Single fork - should work just like a regular stream.
#[tokio::test]
async fn single() {
    let fork = create_test_stream().fork();

    let data = fork.collect::<Vec<_>>().await;
    assert_eq!(data, [0, 1, 2, 3]);
}

/// Multiple forks polled concurrently - each fork should yield all of the items from the underlying stream.
#[tokio::test]
async fn multiple_concurrently() {
    let fork1 = create_test_stream().fork();
    let fork2 = fork1.clone();
    let concurrent = stream::select(fork1, fork2);

    let data = concurrent.collect::<Vec<_>>().await;
    assert_eq!(data, [0, 0, 1, 1, 2, 2, 3, 3]);
}

/// Multiple forks pulled consequtively - each fork should yield all of the items from underlying stream.
#[tokio::test]
async fn multiple_consequtively() {
    let fork1 = create_test_stream().fork();
    let fork2 = fork1.clone();

    let data = fork1.collect::<Vec<_>>().await;
    assert_eq!(data, [0, 1, 2, 3]);

    let data = fork2.collect::<Vec<_>>().await;
    assert_eq!(data, [0, 1, 2, 3]);
}

/// One of the forks falls behind the other by a few elements - elements are to be buffered and garbage collected as the stream falling behind advances.
#[tokio::test]
async fn fall_behind() {
    let mut fork1 = create_test_stream().fork();
    let mut fork2 = fork1.clone();

    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    assert_eq!(fork2.next().await.unwrap(), 0);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    assert_eq!(fork2.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");
}

/// fork2 falls behind fork2, but eventually "outpaces" it - both forks should yield all items as expected.
#[tokio::test]
async fn fork_outpaces_source() {
    let mut fork1 = create_test_stream().fork();
    let mut fork2 = fork1.clone();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    assert_eq!(fork2.next().await.unwrap(), 0);
    assert_eq!(fork2.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");

    assert_eq!(fork2.next().await.unwrap(), 2);
    assert_eq!(fork2.next().await.unwrap(), 3);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 2,
            refs: 1,
        },
        BufferEntry {
            item: 3,
            refs: 1,
        },
    ]
    "###);
}

/// A new fork is introduced when the main has already been polled a few times - new fork should fall behind underlying stream by the same number of items
/// as the original one.
#[tokio::test]
async fn fork_created_late() {
    let mut fork1 = create_test_stream().fork();
    let mut fork2 = fork1.clone();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    let mut fork3 = fork2.clone();
    assert_eq!(fork2.next().await.unwrap(), 0);
    assert_eq!(fork2.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    assert_eq!(fork3.next().await.unwrap(), 0);
    assert_eq!(fork3.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");
}

/// A fork is dropped when the main has already been polled a few times - items waiting to be polled by the fork should be garbage collected.
#[tokio::test]
async fn fork_dropped_late() {
    let mut fork1 = create_test_stream().fork();
    let fork2 = fork1.clone();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    drop(fork2);

    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");
}

#[tokio::test]
/// A fork is created from a Weak - resulting fork should be as advanced as the most advanced fork.
async fn weak() {
    let mut fork1 = create_test_stream().fork();
    let fork2 = fork1.clone();
    let weak = fork2.downgrade();

    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
    ]
    "###);

    let mut fork3 = weak.upgrade().unwrap();
    assert_eq!(fork3.next().await.unwrap(), 2);
    assert_eq!(fork3.next().await.unwrap(), 3);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 0,
            refs: 1,
        },
        BufferEntry {
            item: 1,
            refs: 1,
        },
        BufferEntry {
            item: 2,
            refs: 2,
        },
        BufferEntry {
            item: 3,
            refs: 2,
        },
    ]
    "###);
}

#[tokio::test]
/// A `Weak` is created from a fork - items are still garbage collected as normal, as if the `Weak` didn't exist.
async fn weak_does_not_affect_garbage_collection() {
    let mut fork1 = create_test_stream().fork();
    let weak = fork1.downgrade();

    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");
    drop(weak);
}

#[tokio::test]
/// A fork created from a Weak falls behind - items that it needs to read should not be garbage collected.
async fn weak_falls_behind() {
    let mut fork1 = create_test_stream().fork();
    let weak = fork1.downgrade();

    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @"[]");

    let fork2 = weak.upgrade().unwrap();
    assert_eq!(fork1.next().await.unwrap(), 2);
    assert_eq!(fork1.next().await.unwrap(), 3);
    assert_debug_snapshot!(fork1.inner.lock().unwrap().buffer.buf, @r###"
    [
        BufferEntry {
            item: 2,
            refs: 1,
        },
        BufferEntry {
            item: 3,
            refs: 1,
        },
    ]
    "###);
    drop(fork2);
}

#[test]
/// Attempting to create a fork from a Weak when all existing forks have been dropped - should return `None`.
fn weak_none() {
    let fork1 = create_test_stream().fork();
    let weak = fork1.downgrade();
    drop(fork1);
    assert!(weak.upgrade().is_none());
}

/// Source stream never returned `None` - fork should not be terminated.
#[tokio::test]
async fn fused_has_data_from_source() {
    let mut fork1 = create_test_stream().fork();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_eq!(fork1.next().await.unwrap(), 2);
    assert_eq!(fork1.next().await.unwrap(), 3);
    assert!(!fork1.is_terminated());
}

/// Source stream returned `None`, but there are still buffered items that haven't been yielded - fork should not be terminated.
#[tokio::test]
async fn fused_has_data_from_buffer() {
    let mut fork1 = create_test_stream().fork();
    let fork2 = fork1.clone();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_eq!(fork1.next().await.unwrap(), 2);
    assert_eq!(fork1.next().await.unwrap(), 3);
    assert_eq!(fork1.next().await, None);
    assert!(!fork2.is_terminated());
}

/// Source stream returned `None` and there are no buffered items that have not been yielded - fork should be terminated.
#[tokio::test]
async fn fused_has_no_data() {
    let mut fork1 = create_test_stream().fork();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_eq!(fork1.next().await.unwrap(), 2);
    assert_eq!(fork1.next().await.unwrap(), 3);
    assert_eq!(fork1.next().await, None);
    assert!(fork1.is_terminated());
}

/// Size hint when there are no buffered items to be yielded by the fork - should return size hint of underlying stream.
#[test]
fn size_hint_from_source() {
    let fork1 = stream::iter(0..4).fork();
    assert_debug_snapshot!(fork1.size_hint(), @r###"
    (
        4,
        Some(
            4,
        ),
    )
    "###);
}

/// Size hint when there are buffered items to be yielded by the fork - should return size hint of underlying stream + number of buffered items.
#[tokio::test]
async fn test() {
    let mut fork1 = stream::iter(0..4).fork();
    let fork2 = fork1.clone();
    assert_eq!(fork1.next().await.unwrap(), 0);
    assert_eq!(fork1.next().await.unwrap(), 1);
    assert_debug_snapshot!(fork1.size_hint(), @r###"
    (
        2,
        Some(
            2,
        ),
    )
    "###);
    assert_debug_snapshot!(fork2.size_hint(), @r###"
    (
        4,
        Some(
            4,
        ),
    )
    "###)
}
