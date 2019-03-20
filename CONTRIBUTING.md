# Contributing to Reactive-Streams-Commons

Welcome to Reactive-Streams-Commons repository, **Rsc** for short.

Rsc is a joint research project between developer(s) of [RxJava](https://github.com/ReactiveX/RxJava) and [Project Reactor](https://github.com/reactor/reactor-core) to develop highly optimized [Reactive-Streams](https://github.com/reactive-streams/reactive-streams-jvm) compliant operators (sources, intermediate transformations and exit points).

If you'd like to embed Rsc in a library please amend the `generate.gradle` accordingly. Reactor is set as an implementor in this file and you can look it as an example.

## Prerequisites

We have a bad news for you: this project requires **master level knowledge and experience with reactive solutions** (i.e., RxJava, Project Reactor or any other Reactive-Streams library). Knowing most rules of the Reactive-Streams specification is also mandatory (except the controversial ones : and their limitation).

Here are some questions that you should be able to answer beforehand:

  - Can you tell the functional difference between `subscribeOn` and `observeOn` (or `publishOn`)?
  - Can you tell the difference between `map` and `flatMap`?
  - Do you understand the Java Memory Model or the notion of acquire-release pairs and memory barriers?
  - Can you write a properly backpressured, single element Publisher (`just()`)  from scratch?
  - Do you understand the various concurrent queue types and when to use them?
  
It is also advised you read @akarnokd's blog about [Advanced RxJava](https://akarnokd.blogspot.hu/) (beginning with the very first post and moving forward in time). Most RxJava constructs are about a relatively simple transformation away from Reactive-Streams constructs.

## Asking questions

We don't suggest going to StackOverflow and asking questions there (even though we like the points for accepted and liked answers :) because only a few people in the world know about Rsc.

Therefore, you can go to the usual [issues](https://github.com/reactor/reactive-streams-commons/issues) page and:

  - Try to be specific; we may eventually create in-depth documentation or tutorials.
  - Include examples or tests that show your case.
  - We may close dormant or inactive issues after 15-30 days.

## Reporting bugs

You can go to the usual [issues](https://github.com/reactor/reactive-streams-commons/issues) page and:

  - See if there is an issue for it already; check the closed issues as well.
  - When you report a bug, please include a unit test that demonstrates it. It helps us greatly in tracking down an unknown cause.
  - Try to be responsive. Unlike other projects, we usually respond to you almost immediately (unless you catch us at night) and may ask you some clarifications.
  - We may close dormant or inactive issues after 15-30 days.

## Feature requests

We don't want this project to be a dumping ground for all kinds of operators. This project is mainly for operators found in other libraries which are also frequently in use and as such would benefit from optimizations. The question is not if we can build a certain operator - we are confident and experienced enough to know that upfront - but if we can squeeze out at least +15% throughput. 

That being said, we ourselves sometimes find the need for extra operators, for example, to interoperate with standard Java features or simply the complexity of the operator makes it by itself interesting for study from the aspects of our other optimizations.

## Releases

We haven't decided on timing, milestones or releases. From time to time, the snapshot version will be bumped up to the next value.

## Code style

  - 4 spaces, no tabs.
  - Unix style line endings.
  - Add javadoc to at least the main class file.
  - Don't go too long with lines, about 90 characters is okay.
  - Please don't reformat code and make sure your IDE doesn't add/remove whitespaces outside your working area automatically.
  - Don't add file headers just yet.
  - Please don't add `@author` tags; the commit history and blame will list your contributions promptly.

## External dependencies

We'd like to avoid depending on anything else except the standard Reactive-Streams API.

## Submitting pull requests

You can submit pull request the usual way:

  - Clone this repository.
  - Create a branch for your changes, name it as you see fit.
  - Perform the changes:
    - for bugfixes, please add the failing test case to the unit tests
    - for new operators, please add a modest set of tests that includes:
      - argument verifications
      - normal behavior with unbounded consumer
      - backpressured behavior
      - cancellation propagation
      - asynchronous behavior via `subscribeOn` and/or `observeOn`
  - Run the `gradle build` locally
  - Push your branch into your cloned repository
  - GitHub should now offer a create pull-request button
    - update the PR name to be meaningful (instead of the commit text)
    - describe what you did and why
    - include references to related issues if applicable
  - Wait for the Travis-CI to run
    - if it fails, see if it's your changes or unrelated ones
    - rerun the job via the Travis-CI GUI to clear some failures that is caused by slow Travis-CI
  - You can keep working on your branch, adding commits to it. No need to squash your commits anymore, GitHub now supports it automatically
  - Your PR may become non-mergeable; please rebase your branch onto the current master.
    - (in some rare cases, you may have to start out of a fresh checkout from master and redo your changes manually)
