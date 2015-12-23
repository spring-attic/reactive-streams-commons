# reactive-streams-commons
A Repository for commons utilities implementations for Reactive Streams that are not tied to scheduling nor queueing.

## Supported datasources

I.e., converts non-reactive data sources into `Publisher`s

  - `PublisherJust` : emits a single value
  
## Supported transformations

  - `PublisherMap` : map values to other values via a function

## Supported extractions

I.e., these allow leaving the reactive-streams world


## Travis status


<a href='https://travis-ci.org/reactor/reactive-stream-extensions/builds'><img src='https://travis-ci.org/reactor/reactive-stream-extensions.svg?branch=master'></a>
