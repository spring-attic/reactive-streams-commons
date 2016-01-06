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
  - `PublisherUsing` : create a resource, stream values in a Publisher derived from the resource and release the resource when the sequence completes or the Subscriber cancels
  
## Supported transformations

  - `PublisherAccumulate` : Accumulates the source values with an accumulator function and returns the intermediate results of this function application
  - `PublisherAll` : emits a single true if all values of the source sequence match the predicate
  - `PublisherAny` : emits a single true if any value of the source sequence matches the predicate
  - `PublisherBuffer` : buffers certain number of subsequent elements and emits the buffers
  - `PublisherBufferBoundary` : buffers elements into continuous, non-overlapping lists where another Publisher
  signals the start/end of the buffer regions
  - `PublisherBufferStartEnd` : buffers elements into possibly overlapping buffers whose boundaries are determined
  by a start Publisher's element and a signal of a derived Publisher
  - `PublisherCollect` : collects the values into a container and emits it when the source completes
  - `PublisherCombineLatest` : combines the latest values of many sources through a function
  - `PublisherCount` : counts the number of elements the source sequence emits
  - `PublisherDistinct` : filters out elements that have been seen previously according to a custom collection
  - `PublisherDistinctUntilChanged` : filters out subsequent and repeated elements
  - `PublisherDefaultIfEmpty` : emits a single value if the source is empty
  - `PublisherDelaySubscription` : delays the subscription to the main source until the other source signals a value or completes
  - `PublisherDrop` : runs the source in unbounded mode and drops values if the downstream doesn't request fast enough
  - `PublisherElementAt` : emits the element at the specified index location
  - `PublisherFilter` : filters out values which doesn't pass a predicate
  - `PublisherIgnoreElements` : ignores values and passes only the terminal signals along
  - `PublisherIsEmpty` : returns a single true if the source sequence is empty
  - `PublisherLatest` : runs the source in unbounded mode and emits the latest value if the downstream doesn't request fast enough
  - `PublisherLift` : maps the downstream Subscriber into an upstream Subscriber which allows implementing custom operators via lambdas
  - `PublisherMap` : map values to other values via a function
  - `PublisherPeek` : peek into the lifecycle and signals of a stream
  - `PublisherReduce` : aggregates the source values with the help of an accumulator function and emits the the final accumulated value
  - `PublisherRepeat` : repeatedly streams the source sequence fixed or unlimited times
  - `PublisherRepeatPredicate` : repeatedly stream the source if a predicate returns true
  - `PublisherRepeatWhen` : repeats a source when a companion sequence signals an item in response to the main's completion signal
  - `PublisherResume` : if the source fails, the stream is resumed by another Publisher returned by a function for the failure exception
  - `PublisherRetry` : retry a failed source sequence fixed or unlimited times
  - `PublisherRetryPredicate` : retry if a predicate function returns true for the exception
  - `PublisherRetryWhen` : retries a source when a companion sequence signals an item in response to the main's error signal
  - `PublisherSample` : samples the main source whenever the other Publisher signals a value
  - `PublisherScan` : aggregates the source values with the help of an accumulator function and emits the intermediate results
  - `PublisherSingle` : expects the source to emit only a single item
  - `PublisherSkip` : skips a specified amount of values
  - `PublisherSkipLast` : skips the last N elements
  - `PublisherSkipUntil` : skips values until another sequence signals a value or completes
  - `PublisherSkipWhile` skips values while the predicate returns true
  - `PublisherSwitchIfEmpty` : continues with another sequence if the first sequence turns out to be empty.
  - `PublisherSwitchMap` : switches to and streams a Publisher generated via a function whenever the upstream signals a value
  - `PublisherTake` : takes a specified amount of values and completes
  - `PublisherTakeLast` : emits only the last N values the source emitted before its completion
  - `PublisherTakeWhile` : relays values while a predicate returns true for the values (checked before each value)
  - `PublisherTakeUntil` : relays values until another Publisher signals
  - `PublisherTakeUntilPredicate` : relays values until a predicate returns true (checked after each value)
  - `PublisherTimeout` uses per-item `Publisher`s that when they fire mean the timeout for that particular item unless a new item arrives in the meantime
  - `PublisherWindow` : splits the source sequence into possibly overlapping windows of given size
  - `PublisherWithLatestFrom` : combines values from a master source with the latest values of another Publisher via a function
  - `PublisherZipIterable` : pairwise combines a sequence of values with elements from an iterable

## Supported extractions

I.e., these allow leaving the reactive-streams world.


## Travis status


<a href='https://travis-ci.org/reactor/reactive-streams-commons/builds'><img src='https://travis-ci.org/reactor/reactive-streams-commons.svg?branch=master'></a>
