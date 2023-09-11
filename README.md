# fork_stream
Clone any Stream `S` where `<S as Stream>::Item: Clone`

## Usage

```rust
use fork_stream::StreamExt as _;

async fn example() {
    let source = futures::stream::iter(0..3);

    let fork1 = source.fork();

    let fork2 = fork1.clone();

    assert_eq!(fork1.collect(), vec![0, 1, 2]);

    assert_eq!(fork2.collect(), vec![0, 1, 2]);
}
```

### Behavior

1. Polled items from the source stream are stored in a buffer;
2. When forks of the source stream are polled, they either yield clones of the items from the buffer,
or poll the source stream for new items;
4. If there are no longer any forks that may read an item (either because the fork is dropped, or because
all of the forks have already yielded that item) the item is dropped;
5. Whenever possible, items from the buffer are moved out instead of clonned.
6. When all of the forks are dropped, source stream is dropped.

### `Weak`

Any fork can be downgraded to a [`Weak`], which can later be upgraded back, similar to [`std::rc::Rc`] or [`std::sync::Arc`] APIs.

This behaves as follows:

1. [`Weak`] does not implement [`Stream`] and cannot be polled without being upgraded first;
2. When a [`Weak`] is upgraded into a [`Forked`], the resulting [`Forked`] is as advanced as the source stream;
i.e. it will not yield any items that had been yielded by any other forks prior to the upgrade.
3. If all of the forks had been dropped prior to the upgrade, `Weak::upgrade` returns `None`.

`Weak` API is useful when you want to reuse streams that are expensive to intialize,
but also want to drop them when they are not needed.

### Differences from `shared_stream`

This library implements an API similar to that of [`shared_stream`](https://docs.rs/shared_stream), with a few notable differences:

1. Streams produced by this library are [`Send`] and [`Sync`]. For this reason we have to use synchronisation primitives that
support it, which may be less performant, but makes it a more suitable option for async environments.
2. `shared_stream` buffers the items for as long as at least one clone of the source stream exists. This library "garbage collects"
the items as soon as possible. This comes at a cost of some extra business logic, which may be less performant, but makes it a more
suitable option for situations where streams are supposed to be long-lived, such as servers.
3. This library provides a [`Weak`] API, see above.
