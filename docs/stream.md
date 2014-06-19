### stream basics

A Manifold stream can be created using `manifold.stream/stream`:

```clj
> (require '[manifold.stream :as s])
nil
> (def s (s/stream))
#'s
```

A stream can be thought of as two separate halves: a **sink**  which consumes messages, and a **source**  which produces them.  We can `put!` messages into the sink, and `take!` them from the source:

```clj
> (s/put! s 1)
#<Deferred: :pending>
> (s/take! s)
#<SuccessDeferred: 1>
```

Notice that both `put!` and `take!` return [deferred values](/docs/deferred.md).  The deferred returned by `put!` will yield `true` if the message was accepted by the stream, and `false` otherwise; the deferred returned by `take!` will yield the message.

Sinks can be **closed** by calling `close!`, which means they will no longer accept messages.

```clj
> (s/close! s)
nil
> @(s/put! s 1)
false
```

We can check if a sink is closed by calling `closed?`, and register a no-arg callback using `on-closed` to be notified when the sink is closed.

Sources that will never produce any more messages (often because the corresponding sink is closed) is said to be **drained**.  We may check whether a source is drained via `drained?` and `on-drained`.

By default, calling `take!` on a drained source will yield a message of `nil`.  However, if `nil` is a valid message, we may want to specify some other return value to denote that the source is drained:

```clj
> @(s/take! s ::drained)
::drained
```

We may also want to put a time limit on how long we're willing to wait on our put or take to complete.  For this, we can use `try-put!` and `try-take!`:

```clj
> (def s (s/stream))
#'s
> @(s/try-put! s :foo 1000 ::timeout)
::timeout
```

Here we try to put a message into the stream, but since there are no consumers, it will fail after waiting for 1000ms.  Here we've specified `::timeout` as our special timeout value, otherwise it would simply return `false`.

```clj
> @(s/try-take! s ::drained 1000 ::timeout)
::timeout
```

Again, we specify the timeout and special timeout value.  When using `try-take!`, we must specify return values for both the drained and timeout outcomes.

### stream operators

The simplest thing we can do a stream is consume every message that comes into it:

```clj
> (consume #(prn 'message! %) s)
true
> @(s/put! s 1)
< message! 1 >
true
```

However, we can also create derivative streams using operators analogous to Clojure's sequence operators:

```clj
> (->> [1 2 3]
    s/lazy-seq->stream
    (s/map inc)
    s/stream->lazy-seq)
(2 3 4)
```

Here, we've mapped `inc` over a stream, transforming from a sequence to a stream and then back to a sequence for the sake of a concise example.  Note that we can create multiple derived streams from the same source:

```clj
> (def s (s/stream))
#'s
> (def a (s/map inc s))
#'a
> (def b (s/map dec s))
#'b
> @(s/put! s 0)
true
> @(s/take! a)
1
> @(s/take! b)
-1
```

Here, we create a source stream `s`, and map `inc` and `dec` over it.  When we put our message into `s` it immediately is accepted, since `a` and `b` are downstream.  All messages put into `s` will be propagated into *both* `a` and `b`.

If `s` is closed, both `a` and `b` will be closed, as will any other downstream sources we've created.  Likewise, if everything downstream of `s` is closed, `s` will also be closed.  This is almost always desirable, as failing to do this will simply cause `s` to exert backpressure on everything upstream of it.  However, If we wish to avoid this behavior, we can create a `(permanent-stream)`, which cannot be closed.

Manifold provides a number of stream operators, including `map`, `filter`, `mapcat`, `buffer`, and `batch`.

### connecting streams

Having created an event source through composition of operators, we will often want to feed all messages into a sink.  This can be accomplished via `connect`:

```clj
> (def a (s/stream))
#'a
> (def b (s/stream))
#'b
> (s/connect a b)
true
> @(s/put! a 1)
true
> @(s/take! b)
1
```

Again, we see that our message is immediately accepted into `a`, and can be read from `b`.  We may also pass an options map into `connect`, with any of the following keys:

| field | description |
|-------|-------------|
| `upstream?` | whether the

### buffers and backpressure

We saw above that if we attempt to put a message into a stream, it won't succeed until the value is taken out.  This is because the default stream has no buffer; it simply conveys messages from producers to consumers.  If we want
