### 0.4.4

* manifold now requires Clojure 1.10 or later due to use of `requiring-resolve`

Contributions by Allen Rohner

### 0.4.3

* Improved clj-kondo exports

Contributions by Jacob Maine and Ferdinand Beyer

### 0.4.2

Contributions by Matthew Davidson, Eric Dvorsak, and Arnaud Geiser

* Fixes a `print-method` dispatch hierarchy bug when also using Promesa.

### 0.4.1

Contributions by Ryan Smith

* Fixes a bug in `go-off` when using newer versions of core.async.

### 0.4.0

Contributions by Renan Ribeiro, Matthew Davidson, and Arnaud Geiser

* Manifold deferreds now implement CompletionStage
* Add clj-kondo support to many macros
* Clarify how/when upstreams are automatically closed
* Many docstring improvements
* Fixed bug in `go-off` test

Special thanks to newcomer Renan Ribeiro for his major contributions to this release!

### 0.3.0

Contributions by Ryan Schmukler and Matthew Davidson

* Added new windowing stream fns, `sliding-stream` and `dropping-stream`
* Java 8 is now the minimum version supported
* Fixed accidental usage of core `realized?` in `stream->seq`
* Fixed broken BaseStream test
* Update CircleCI Docker image
* Update Dirigiste, tools.logging, riddley, and core.async versions
* Add deps.edn usage instructions
* Removed Codox
* Cleaned up timing tests and ns metadata

### 0.2.4

Contributions by Arnaud Geiser

* Support custom `Thread` classes with Manifold's executor

### 0.2.3

Contributions by Matthew Davidson

* Added `manifold.go-off` to generated documentation

### 0.2.2

Contributions by Matthew Davidson

* Renamed `go-off-executor` to `go-off-with` to match existing `future-with` macro

### 0.2.1

Contributions by Matthew Davidson, Ryan Smith

* Fixed bug in `go-off-executor` macro with fully-qualified class name

### 0.2.0

Contributions by Matthew Davidson, Ryan Smith

* Add `go-off`, a `core-async`-style macro with a manifold flavor. Big thanks to Ryan Smith and Yummly for contributing this!
* Switch to `bound-fn` in `let-flow` to fix bug where dynamic vars were incorrect for other threads
* Modernized indentation to match current Clojure styles and fix misalignments

### 0.1.9

Contributions by Erik Assum, Reynald Borer, Matthew Davidson, Alexey Kachayev, led, Dominic Monroe, Pierre-Yves Ritschard, Ryan Smith, Justin Sonntag, Zach Tellman, Luo Tian, and Philip van Heerden.

* Updated docs to use cljdoc.org by default
* Minor doc improvements
* Bumped up dependencies to modern versions
* Convert to CircleCI for testing and remove `jammin`
* Set up for clj-commons
* Fix bug where excessive pending takes return wrong deferred
* Clean up timed-out pending takes and exposes vars to control clean-up behavior
* Remove Travis CI
* Allow functions passed to `time/in` to return a deferred
* Make `time/in` cancellable
* Extend thread-factory builder to create non-daemon threads
* Prevent `let-flow` body from executing on last deferred thread
* Fix bug in clock argument order
* Remove `timeout` future execution if deferred completes before timeout
* Fix bug using `let-flow` in `loop`

### 0.1.8

Thanks to Paweł Stroiński

* Fix handling of non-`Throwable` deferred errors when dereferencing

### 0.1.7

Thanks to Ted Cushman, Vadim Platonov

* Increase stack size in the wait-pool
* Fix lifecycle bugs in `throttle`, `partition-all`, and `transform`
* Change `let-flow` to wait on all deferred values, not just the ones used by the body

### 0.1.6

Thanks to Vadim Platonov, Miikka Koskinen, Alex Engelberg, and Oleh Palianytsia

* fix bug in `batch`
* make `reduce` compatible with Clojure's `reduced` short-circuiting
* make sure `catch` can match non-`Throwable` errors
* allow for destructuring in `loop`
* add `alt` mechanism for choosing the first of many deferreds to be realized

### 0.1.5

Thanks to Tsutomu Yano and Joshua Griffith

* fix bugs in `finally` and `consume`

### 0.1.4

* Honor `:thread-factory` parameter in `manifold.executor`.

### 0.1.3

* Target latest Dirigiste, which is no longer compiled using JDK 8 byte code.

### 0.1.2

* fix lifecycle for `batch` and `throttle` when the source is a permanent stream
* fix path where `manifold.stream/reduce` could fail to yield any value when the reducer function throws an exception, rather than yielding that error
* add `mock-clock` and `with-clock` to `manifold.time`, to aid with testing timeouts and other wall-clock behavior
* add `consume-async` method, which expects the consume callback to return a deferred that yields a boolean, rather than simply a boolean value
* small corrections and clarifications to doc-strings


### 0.1.1

* fix inline definition of `on-realized`, which would cause `(on-realized form a b)` to execute `form` twice
* fix coercion support for `java.util.Iterator`
* don't automatically coerce core.async channels to deferreds (use `(take! (->source chan))` instead)
* add coercion support for Java 8 `BasicStream` and `CompletableFuture`, which can be optionally disabled
* add `onto` method to `manifold.stream` to mirror the one in `manifold.deferred`
* add formal, configurable execution model

### 0.1.0

* initial release
