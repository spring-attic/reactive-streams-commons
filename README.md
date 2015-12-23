# reactive-streams-commons
A Repository for commons utilities implementations for Reactive Streams.

## Supported datasources

I.e., converts non-reactive data sources into `Publisher`s.

  - `PublisherArray` : emits the elements of an array
  - `PublisherCallable` : emits a single value returned by a `Callable`
  - `PublisherCompletableFuture` : emits a single value produced by a `CompletableFuture`
  - `PublisherDefer` : calls a `Supplier` to create the actual `Publisher` the `Subscriber` will be subscribed to.
  - `PublisherEmpty` : does not emit any value and calls `onCompleted`; use `instance()` to get its singleton instance with the proper type parameter
  - `PublisherError` : emits a constant or generated Throwable exception
  - `PublisherIterable` : emits the elements of an `Iterable`
  - `PublisherJust` : emits a single value
  - `PublisherNever` : doesn't emit any signal other than `onSubscribe`; use `instance()` to get its singleton instance with the proper type parameter
  - `PublisherRange` : emits a range of integer values
  - `PublisherStream` : emits elements of a `Stream`
  
## Supported transformations

  - `PublisherMap` : map values to other values via a function

## Supported extractions

I.e., these allow leaving the reactive-streams world.


## Travis status


<a href='https://travis-ci.org/reactor/reactive-streams-commons/builds'><img src='https://travis-ci.org/reactor/reactive-streams-commons.svg?branch=master'></a>
