### 0.1.0

* initial release

### 0.1.1

* fix inline definition of `on-realized`, which would cause `(on-realized form a b)` to execute `form` twice
* fix coercion support for `java.util.Iterator`
* don't automatically coerce core.async channels to deferreds (use `(take! (->source chan))` instead)
* add coercion support for Java 8 `BasicStream` and `CompletableFuture`, which can be optionally disabled
* add `onto` method to `manifold.stream` to mirror the one in `manifold.deferred`
* add formal, configurable execution model

### 0.1.2

* fix lifecycle for `batch` and `throttle` when the source is a permanent stream
* fix path where `manifold.stream/reduce` could fail to yield any value when the reducer function throws an exception, rather than yielding that error
* add `mock-clock` and `with-clock` to `manifold.time`, to aid with testing timeouts and other wall-clock behavior
* add `consume-async` method, which expects the consume callback to return a deferred that yields a boolean, rather than simply a boolean value
* small corrections and clarifications to doc-strings
