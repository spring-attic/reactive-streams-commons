# reactive-streams-commons
A joint research effort for building highly optimized Reactive-Streams compliant operators.

Java 8 required.

<a href='https://travis-ci.org/reactor/reactive-streams-commons/builds'><img src='https://travis-ci.org/reactor/reactive-streams-commons.svg?branch=master'></a>

## Maven

```
repositories {
    maven { url 'http://repo.spring.io/libs-snapshot' }
}

dependencies {
    compile 'io.projectreactor:reactive-streams-commons:0.5.0.BUILD-SNAPSHOT'
}
```

[Snapshot](http://repo.spring.io/libs-snapshot/io/projectreactor/reactive-streams-commons/) directory.

## Operator-fusion documentation

  - [Operator fusion, 1/2](http://akarnokd.blogspot.hu/2016/03/operator-fusion-part-1.html)
  - [Operator fusion, 2/2](http://akarnokd.blogspot.hu/2016/04/operator-fusion-part-2-final.html)
  - [Fusion Matrix](https://rawgit.com/reactor/reactive-streams-commons/master/fusion-matrix.html)

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
  - `PublisherFuture` : awaits and emits a single value emitted by a `Future`
  - `PublisherGenerate` : generate signals one-by-one via a function 
  - `PublisherInterval` : periodically emits an ever increasing sequence of long values
  - `PublisherIterable` : emits the elements of an `Iterable`
  - `PublisherJust` : emits a single value
  - `PublisherNever` : doesn't emit any signal other than `onSubscribe`; use `instance()` to get its singleton instance with the proper type parameter
  - `PublisherRange` : emits a range of integer values
  - `PublisherStream` : emits elements of a `Stream`
  - `PublisherTimer` : emit a single 0L after a specified amount of time
  - `PublisherUsing` : create a resource, stream values in a Publisher derived from the resource and release the resource when the sequence completes or the Subscriber cancels
  - `PublisherZip` : Repeatedly takes one item from all source Publishers and runs it through a function to produce the output item
  
## Supported transformations

  - `ConnectablePublisherAutoConnect` given a ConnectablePublisher, it connects to it once the given  amount of subscribers subscribed
  - `ConnectablePublisherRefCount` given a ConnectablePublisher, it connects to it once the given amount of subscribers subscribed to it and disconnects once all subscribers cancelled
  - `ConnectablePublisherPublish` : allows dispatching events from a single source to multiple subscribers similar to a Processor but the connection can be manually established or stopped.
  - `PublisherAccumulate` : Accumulates the source values with an accumulator function and returns the intermediate results of this function application
  - `PublisherAggregate` : Aggregates the source values with an aggergator function and emits the last result.
  - `PublisherAll` : emits a single true if all values of the source sequence match the predicate
  - `PublisherAny` : emits a single true if any value of the source sequence matches the predicate
  - `PublisherAwaitOnSubscribe` : makes sure onSubscribe can't trigger the onNext events until it returns
  - `PublisherBuffer` : buffers certain number of subsequent elements and emits the buffers
  - `PublisherBufferBoundary` : buffers elements into continuous, non-overlapping lists where another Publisher
  signals the start/end of the buffer regions
  - `PublisherBufferBoundaryAndSize` : buffers elements into continuous, non-overlapping lists where the each buffer is emitted when they become full or another Publisher signals the boundary of the buffer regions
  - `PublisherBufferStartEnd` : buffers elements into possibly overlapping buffers whose boundaries are determined
  by a start Publisher's element and a signal of a derived Publisher
  - `PublisherCollect` : collects the values into a container and emits it when the source completes
  - `PublisherCombineLatest` : combines the latest values of many sources through a function
  - `PublisherConcatMap` : Maps each upstream value into a Publisher and concatenates them into one sequence of items
  - `PublisherCount` : counts the number of elements the source sequence emits
  - `PublisherDistinct` : filters out elements that have been seen previously according to a custom collection
  - `PublisherDistinctUntilChanged` : filters out subsequent and repeated elements
  - `PublisherDefaultIfEmpty` : emits a single value if the source is empty
  - `PublisherDelaySubscription` : delays the subscription to the main source until the other source signals a value or completes
  - `PublisherDetach` : detaches the both the child Subscriber and the Subscription on termination or cancellation.
  - `PublisherDrop` : runs the source in unbounded mode and drops values if the downstream doesn't request fast enough
  - `PublisherElementAt` : emits the element at the specified index location
  - `PublisherFilter` : filters out values which doesn't pass a predicate
  - `PublisherFlatMap` : maps a sequence of values each into a Publisher and flattens them back into a single sequence, interleaving events from the various inner Publishers
  - `PublisherFlattenIterable` : concatenates values from Iterable sequences generated via a mapper function
  - `PublisherGroupBy` : groups source elements into their own Publisher sequences via a key function
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
  - `PublisherSkipWhile`: skips values while the predicate returns true
  - `PublisherStreamCollector` : Collects the values from the source sequence into a `java.util.stream.Collector` instance; see `Collectors` utility class in Java 8+
  - `PublisherSwitchIfEmpty` : continues with another sequence if the first sequence turns out to be empty.
  - `PublisherSwitchMap` : switches to and streams a Publisher generated via a function whenever the upstream signals a value
  - `PublisherTake` : takes a specified amount of values and completes
  - `PublisherTakeLast` : emits only the last N values the source emitted before its completion
  - `PublisherTakeWhile` : relays values while a predicate returns true for the values (checked before each value)
  - `PublisherTakeUntil` : relays values until another Publisher signals
  - `PublisherTakeUntilPredicate` : relays values until a predicate returns true (checked after each value)
  - `PublisherThrottleFirst` : takes a value from upstream then uses the duration provided by a generated Publisher to skip other values until that other Publisher signals
  - `PublisherThrottleTimeout` : emits the last value from upstream only if there were no newer values emitted during the time window provided by a publisher for that particular last value
  - `PublisherTimeout` uses per-item `Publisher`s that when they fire mean the timeout for that particular item unless a new item arrives in the meantime
  - `PublisherWindow` : splits the source sequence into possibly overlapping windows of given size
  - `PublisherWindowBatch` : batches the source sequence into continuous, non-overlapping windows where the length of the windows is determined by a fresh boundary Publisher or a maximum elemenets in that window
  - `PublisherWindowBoundary` : splits the source sequence into continuous, non-overlapping windows where the window boundary is signalled by another Publisher
  - `PublisherWindowBoundaryAndSize` : splits the source sequence into continuous, non-overlapping windows where the window boundary is signalled by another Publisher or if a window received a specified amount of values
  - `PublisherWindowStartEnd` : splits the source sequence into potentially overlapping windows controlled by a
  start Publisher and a derived end Publisher for each start value
  - `PublisherWithLatestFrom` : combines values from a master source with the latest values of another Publisher via a function
  - `PublisherZip` : Repeatedly takes one item from all source Publishers and runs it through a function to produce the output item
  - `PublisherZipIterable` : pairwise combines a sequence of values with elements from an iterable

## Supported extractions

I.e., these allow leaving the reactive-streams world.

  - `BlockingIterable` : an iterable that consumes a Publisher in a blocking fashion
  - `BlockingFuture` : can return a future that consumes the source entierly and returns the very last value
  - `BlockingStream` : allows creating sequential and parallel j.u.stream.Stream flows out of a source Publisher
  - `PublisherBase.blockingFirst` : returns the very first value of the source, blocking if necessary; returns null for an empty sequence.
  - `PublisherBase.blockingLast` : returns the very last value of the source, blocking if necessary; returns null for an empty sequence.
  - `PublisherBase.peekLast` : returns the last value of a synchronous source or likely null for other or empty sequences.
