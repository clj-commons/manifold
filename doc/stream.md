### Stream basics

A Manifold stream can be created using `manifold.stream/stream`:

```clojure
> (require '[manifold.stream :as s])
nil
> (def s (s/stream))
#'s
```

A stream can be thought of as two separate halves: a **sink**  which consumes messages, and a **source**  which produces them.  We can `put!` messages into the sink, and `take!` them from the source:

```clojure
> (s/put! s 1)
<< ... >>
> (s/take! s)
<< 1 >>
```

Notice that both `put!` and `take!` return [deferred values](/doc/deferred.md).  The deferred returned by `put!` will yield `true` if the message was accepted by the stream, and `false` otherwise; the deferred returned by `take!` will yield the message.

Sinks can be **closed** by calling `close!`, which means they will no longer accept messages.

```clojure
> (s/close! s)
nil
> @(s/put! s 1)
false
```

We can check if a sink is closed by calling `closed?`, and register a no-arg callback using `on-closed` to be notified when the sink is closed.

Sources that will never produce any more messages (often because the corresponding sink is closed) are said to be **drained**.  We may check whether a source is drained via `drained?` and register callbacks with `on-drained`.

By default, calling `take!` on a drained source will yield a message of `nil`.  However, if `nil` is a valid message, we may want to specify some other return value to denote that the source is drained:

```clojure
> @(s/take! s ::drained)
::drained
```

We may also want to put a time limit on how long we're willing to wait on our put or take to complete.  For this, we can use `try-put!` and `try-take!`:

```clojure
> (def s (s/stream))
#'s
> @(s/try-put! s :foo 1000 ::timeout)
::timeout
```

Here we try to put a message into the stream, but since there are no consumers, it will fail after waiting for 1000ms.  Here we've specified `::timeout` as our special timeout value, otherwise it would simply return `false`.

```clojure
> @(s/try-take! s ::drained 1000 ::timeout)
::timeout
```

Again, we specify the timeout and special timeout value.  When using `try-take!`, we must specify return values for both the drained and timeout outcomes.

### Stream operators

The simplest thing we can do with a stream is consume every message that comes into it:

```clojure
> (s/consume #(prn 'message! %) s)
nil
> @(s/put! s 1)
message! 1
true
```

However, we can also create derivative streams using operators analogous to Clojure's sequence operators, a full list of which [can be found here](https://cljdoc.org/d/manifold/manifold).

```clojure
> (->> [1 2 3]
       s/->source
       (s/map inc)
       s/stream->seq)
(2 3 4)
```

Here, we've mapped `inc` over a stream, transforming from a sequence to a stream and then back to a sequence for the sake of a concise example.  Note that calling `manifold.stream/map` on a sequence will automatically call `->source`, so we can actually omit that, leaving just:

```clojure
> (->> [1 2 3]
       (s/map inc)
       s/stream->seq)
(2 3 4)
```

Since streams are not immutable, in order to treat it as a sequence we must do an explicit transformation via `stream->seq`:

```clojure
> (->> [1 2 3]
       s/->source
       s/stream->seq
       (map inc))
(2 3 4)
```

Note that we can create multiple derived streams from the same source:

```clojure
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

For any Clojure operation that doesn't have an equivalent in `manifold.stream`, we can use `manifold.stream/transform` with a transducer:

```clojure
> (->> [1 2 3]
       (s/transform (map inc))
       s/stream->seq)
(2 3 4)
```

There's also `(periodically period f)`, which behaves like `(repeatedly f)`, but will emit the result of `(f)` every `period` milliseconds.


### Connecting streams

Having created an event source through composition of operators, we will often want to feed all messages into a sink.  This can be accomplished via `connect`:

```clojure
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

| Field | Description |
|-------|-------------|
| `downstream?` | whether the source closing will close the sink, defaults to `true` |
| `upstream?` | whether the sink closing will close the source, *even if there are other sinks downstream of the source*, defaults to `false` |
| `timeout` | the maximum time that will be spent waiting to convey a message into the sink before the connection is severed, defaults to `nil` |
| `description` | a description of the connection between the source and sink, useful for introspection purposes |

After connecting two streams, we can inspect any of the streams using `description`, and follow the flow of data using `downstream`:

```clojure
> (def a (s/stream))
#'a
> (def b (s/stream))
#'b
> (s/connect a b {:description "a connection"})
nil
> (s/description a)
{:pending-puts 0, :drained? false, :buffer-size 0, :permanent? false, ...}
> (s/downstream a)
(["a connection" << stream: ... >>])
```

We can recursively apply `downstream` to traverse the entire topology of our streams.  This can be a powerful way to reason about the structure of our running processes. 

Sometimes we want to change the message from the source before it's placed into the sink.  For this, we can use `connect-via`:

```clojure
> (def a (s/stream))
#'a
> (def b (s/stream))
#'b
> (s/connect-via a #(s/put! b (inc %)) b)
nil
```

Note that `connect-via` takes an argument between the source and sink, which is a single-argument callback.  This callback will be invoked with messages from the source, under the assumption that they will be propagated to the sink.  This is the underlying mechanism for `map`, `filter`, and other stream operators; it allow us to create complex operations that are visible via `downstream`:

```clojure
> (def a (s/stream))
#'a
> (s/map inc a)
<< source: ... >>
> (s/downstream a)
([{:op "map"} << sink: {:type "callback"} >>])
```

Each element returned by `downstream` is a 2-tuple, the first element describing the connection, and the second element describing the stream it's feeding into.

The value returned by the callback for `connect-via` provides backpressure - if a deferred value is returned, further messages will not be passed in until the deferred value is realized.

### Buffers and backpressure

We saw above that if we attempt to put a message into a stream, it won't succeed until the value is taken out.  This is because the default stream has no buffer; it simply conveys messages from producers to consumers.  If we want to create a stream with a buffer, we can simply call `(stream buffer-size)`.  We can also call `(buffer size stream)` to create a buffer downstream of an existing stream.

We may also call `(buffer metric limit stream)`, if we don't want to measure our buffer's size in messages.  If, for instance, each message is a collection, we could use `count` as our metric, and set `limit` to whatever we want the maximum aggregate count to be.

To limit the rate of messages from a stream, we can use `(throttle max-rate stream)`.

### Event buses and publish/subscribe models

Manifold provides a simple publish/subscribe mechanism in the `manifold.bus` namespace.  To create an event bus, we can use `(event-bus)`.  To publish to a particular topic on that bus, we use `(publish! bus topic msg)`.  To get a stream representing all messages on a topic, we can call `(subscribe bus topic)`.

Calls to `publish!` will return a deferred that won't be realized until all streams have accepted the message.  By default, all streams returned by `subscribe` are unbuffered, but we can change this by providing a `stream-generator` to `event-bus`, such as `(event-bus #(stream 1e3))`.  A short example of how `event-bus` can be used in concert with the buffering and flow control mechanisms [can be found here](https://youtu.be/1bNOO3xxMc0?t=1887).
