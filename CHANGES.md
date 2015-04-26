### 0.1.0

* initial release

### 0.1.1

* don't automatically coerce core.async channels to deferreds (use `(take! (->source chan))` instead)
* add coercion support for Java 8 `BasicStream` and `CompletableFuture`
* add `onto` method to `manifold.stream` to mirror the one in `manifold.deferred`
* fix coercion support for `java.util.Iterator`
