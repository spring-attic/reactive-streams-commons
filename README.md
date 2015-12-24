# reactive-streams-commons
A Repository for commons utilities implementations for Reactive Streams.

## Supported datasources

I.e., converts non-reactive data sources into `Publisher`s.

  - `PublisherAmb` : relays signals of that source Publisher which responds first with any signal
  - `PublisherArray` : emits the elements of an array
  - `PublisherCallable` : emits a single value returned by a `Callable`
  - `PublisherCompletableFuture` : emits a single value produced by a `CompletableFuture`
  - `PublisherConcatArray` : concatenate an array of `Publisher`s
  - `PublisherConcatIterable` : concatenate an `Iterable` sequence of `Publisher`s
  - `PublisherDefer` : calls a `Supplier` to create the actual `Publisher` the `Subscriber` will be subscribed to.
  - `PublisherEmpty` : does not emit any value and calls `onCompleted`; use `instance()` to get its singleton instance with the proper type parameter
  - `PublisherError` : emits a constant or generated Throwable exception
  - `PublisherGenerate` : generate signals one-by-one via a function 
  - `PublisherIterable` : emits the elements of an `Iterable`
  - `PublisherJust` : emits a single value
  - `PublisherNever` : doesn't emit any signal other than `onSubscribe`; use `instance()` to get its singleton instance with the proper type parameter
  - `PublisherRange` : emits a range of integer values
  - `PublisherStream` : emits elements of a `Stream`
  
## Supported transformations

  - `PublisherAccumulate` : Accumulates the source values with an accumulator function and returns the intermediate results of this function application
  - `PublisherAll` : emits a single true if all values of the source sequence match the predicate
  - `PublisherAny` : emits a single true if any value of the source sequence matches the predicate
  - `PublisherCollect` : collects the values into a container and emits it when the source completes
  - `PublisherCount` : counts the number of elements the source sequence emits
  - `PublisherDefaultIfEmpty` : emits a single value if the source is empty
  - `PublisherFilter` : filters out values which doesn't pass a predicate
  - `PublisherIsEmpty` : returns a single true if the source sequence is empty
  - `PublisherLift` : maps the downstream Subscriber into an upstream Subscriber which allows implementing custom operators via lambdas
  - `PublisherMap` : map values to other values via a function
  - `PublisherRepeat` : repeatedly streams the source sequence fixed or unlimited times
  - `PublisherRetry` : retry a failed source sequence fixed or unlimited times
  - `PublisherScan` : aggregates the source values with the help of an accumulator function and emits the intermediate results
  - `PublisherSkip` : skips a specified amount of values
  - `PublisherSkipWhile` skips values while the predicate returns true
  - `PublisherSkipUntil` : skips values until another sequence signals a value or completes
  - `PublisherSwitchIfEmpty` : continues with another sequence if the first sequence turns out to be empty.
  - `PublisherTake` : takes a specified amount of values and completes
  - `PublisherTakeWhile` : relays values while a predicate returns true for the values (checked before each value)
  - `PublisherTakeUntil` : relays values until another Publisher signals
  - `PublisherTakeUntilPredicate` : relays values until a predicate returns true (checked after each value)
  - `PublisherWithLatestFrom` : combines values from a master source with the latest values of another Publisher via a function

## Supported extractions

I.e., these allow leaving the reactive-streams world.


## Travis status


<a href='https://travis-ci.org/reactor/reactive-streams-commons/builds'><img src='https://travis-ci.org/reactor/reactive-streams-commons.svg?branch=master'></a>
