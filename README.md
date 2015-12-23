# reactive-streams-commons
A Repository for commons utilities implementations for Reactive Streams.

## Supported datasources

I.e., converts non-reactive data sources into `Publisher`s.

  - `PublisherJust` : emits a single value
  - `PublisherArray` : emits the elements of an array
  - `PublisherIterable` : emits the elements of an `Iterable`
  
## Supported transformations

  - `PublisherMap` : map values to other values via a function

## Supported extractions

I.e., these allow leaving the reactive-streams world.


## Travis status


<a href='https://travis-ci.org/reactor/reactive-streams-commons/builds'><img src='https://travis-ci.org/reactor/reactive-streams-commons.svg?branch=master'></a>
