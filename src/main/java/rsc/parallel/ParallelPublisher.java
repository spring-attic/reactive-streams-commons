package rsc.parallel;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

import org.reactivestreams.*;

import rsc.publisher.*;
import rsc.publisher.PublisherConcatMap.ErrorMode;
import rsc.scheduler.Scheduler;
import rsc.util.EmptySubscription;

/**
 * Abstract base class for Parallel publishers that take an array of Subscribers.
 * <p>
 * Use {@code fork()} to start processing a regular Publisher in 'rails'.
 * Use {@code runOn()} to introduce where each 'rail' shoud run on thread-vise.
 * Use {@code join()} to merge the sources back into a single Publisher.
 * 
 * @param <T> the value type
 */
public abstract class ParallelPublisher<T> {
    
    /**
     * Subscribes an array of Subscribers to this ParallelPublisher and triggers
     * the execution chain for all 'rails'.
     * 
     * @param subscribers the subscribers array to run in parallel, the number
     * of items must be equal to the parallelism level of this ParallelPublisher
     */
    public abstract void subscribe(Subscriber<? super T>[] subscribers);
    
    /**
     * Returns the number of expected parallel Subscribers.
     * @return the number of expected parallel Subscribers
     */
    public abstract int parallelism();
    
    /**
     * Returns true if the parallel sequence has to be ordered when joining back.
     * @return true if the parallel sequence has to be ordered when joining back
     */
    public abstract boolean isOrdered();
    
    /**
     * Validates the number of subscribers and returns true if their number
     * matches the parallelism level of this ParallelPublisher.
     * 
     * @param subscribers the array of Subscribers
     * @return true if the number of subscribers equals to the parallelism level
     */
    protected final boolean validate(Subscriber<?>[] subscribers) {
        int p = parallelism();
        if (subscribers.length != p) {
            for (Subscriber<?> s : subscribers) {
                EmptySubscription.error(s, new IllegalArgumentException("parallelism = " + p + ", subscribers = " + subscribers.length));
            }
            return false;
        }
        return true;
    }

    /**
     * Take a Publisher and prepare to consume it on multiple 'rails' (number of CPUs) in a round-robin fashion.
     * @param <T> the value type
     * @param source the source Publisher
     * @return the ParallelPublisher instance
     */
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source) {
        return fork(source, false, Runtime.getRuntime().availableProcessors(), Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    /**
     * Take a Publisher and prepare to consume it on multiple 'rails' (number of CPUs), possibly ordered and round-robin fashion.
     * @param <T> the value type
     * @param source the source Publisher
     * @param ordered if converted back to a Publisher, should the end result be ordered?
     * @return the ParallelPublisher instance
     */
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered) {
        return fork(source, ordered, Runtime.getRuntime().availableProcessors(), Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    /**
     * Take a Publisher and prepare to consume it on parallallism number of 'rails' , 
     * possibly ordered and round-robin fashion.
     * @param <T> the value type
     * @param source the source Publisher
     * @param ordered if converted back to a Publisher, should the end result be ordered?
     * @param parallelism the number of parallel rails
     * @return the new ParallelPublisher instance
     */
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered, int parallelism) {
        return fork(source, ordered, parallelism, Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    /**
     * Take a Publisher and prepare to consume it on parallallism number of 'rails' , 
     * possibly ordered and round-robin fashion and use custom prefetch amount and queue
     * for dealing with the source Publisher's values.
     * @param <T> the value type
     * @param source the source Publisher
     * @param ordered if converted back to a Publisher, should the end result be ordered?
     * @param parallelism the number of parallel rails
     * @param prefetch the number of values to prefetch from the source
     * @param queueSupplier the queue structure supplier to hold the prefetched values from
     * the source until there is a rail ready to process it.
     * @return the new ParallelPublisher instance
     */
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered, 
            int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism > 0 required but it was " + parallelism);
        }
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        
        Objects.requireNonNull(queueSupplier, "queueSupplier");
        Objects.requireNonNull(source, "queueSupplier");

        if (ordered) {
            return new ParallelOrderedSource<>(source, parallelism, prefetch, queueSupplier);
        }
        
        return new ParallelUnorderedSource<>(source, parallelism, prefetch, queueSupplier);
    }

    /**
     * Maps the source values on each 'rail' to another value.
     * <p>
     * Note that the same mapper function may be called from multiple threads concurrently.
     * @param <U> the output value type
     * @param mapper the mapper function turning Ts into Us.
     * @return the new ParallelPublisher instance
     */
    public final <U> ParallelPublisher<U> map(Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        if (isOrdered()) {
            return new ParallelOrderedMap<>((ParallelOrderedBase<T>)this, mapper);
        }
        return new ParallelUnorderedMap<>(this, mapper);
    }
    
    /**
     * Filters the source values on each 'rail'.
     * <p>
     * Note that the same predicate may be called from multiple threads concurrently.
     * @param predicate the function returning true to keep a value or false to drop a value
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        if (isOrdered()) {
            return new ParallelOrderedFilter<>((ParallelOrderedBase<T>)this, predicate);
        }
        return new ParallelUnorderedFilter<>(this, predicate);
    }
    
    /**
     * Specifies where each 'rail' will observe its incoming values with
     * no work-stealing and default prefetch amount.
     * <p>
     * This operator uses the default prefetch size returned by {@code Px.bufferSize()}.
     * <p>
     * The operator will call {@code Scheduler.createWorker()} as many
     * times as this ParallelPublisher's parallelism level is.
     * <p>
     * No assumptions are made about the Scheduler's parallelism level,
     * if the Scheduler's parallelism level is lwer than the ParallelPublisher's,
     * some rails may end up on the same thread/worker.
     * <p>
     * This operator doesn't require the Scheduler to be trampolining as it
     * does its own built-in trampolining logic.
     * 
     * @param scheduler the scheduler to use
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> runOn(Scheduler scheduler) {
        return runOn(scheduler, Px.bufferSize());
    }

    /**
     * Specifies where each 'rail' will observe its incoming values with
     * possibly work-stealing and a given prefetch amount.
     * <p>
     * This operator uses the default prefetch size returned by {@code Px.bufferSize()}.
     * <p>
     * The operator will call {@code Scheduler.createWorker()} as many
     * times as this ParallelPublisher's parallelism level is.
     * <p>
     * No assumptions are made about the Scheduler's parallelism level,
     * if the Scheduler's parallelism level is lwer than the ParallelPublisher's,
     * some rails may end up on the same thread/worker.
     * <p>
     * This operator doesn't require the Scheduler to be trampolining as it
     * does its own built-in trampolining logic.
     * 
     * @param scheduler the scheduler to use
     * that rail's worker has run out of work.
     * @param prefetch the number of values to request on each 'rail' from the source
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> runOn(Scheduler scheduler, int prefetch) {
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        Objects.requireNonNull(scheduler, "scheduler");
        if (isOrdered()) {
            return new ParallelOrderedRunOn<>((ParallelOrderedBase<T>)this, scheduler, prefetch, Px.defaultQueueSupplier(prefetch));
        }
        return new ParallelUnorderedRunOn<>(this, scheduler, prefetch, Px.defaultQueueSupplier(prefetch));
    }

    /**
     * Reduces all values within a 'rail' and across 'rails' with a reducer function into a single
     * sequential value.
     * <p>
     * Note that the same reducer function may be called from multiple threads concurrently.
     * @param reducer the function to reduce two values into one.
     * @return the new Px instance emitting the reduced value or empty if the ParallelPublisher was empty
     */
    public final Px<T> reduce(BiFunction<T, T, T> reducer) {
        Objects.requireNonNull(reducer, "reducer");
        // FIXME is there a reasonable ordered output of this?
        return new ParallelReduceFull<>(this, reducer);
    }
    
    /**
     * Reduces all values within a 'rail' to a single value (with a possibly different type) via
     * a reducer function that is initialized on each rail from an initialSupplier value.
     * <p>
     * Note that the same mapper function may be called from multiple threads concurrently.
     * @param <R> the reduced output type
     * @param initialSupplier the supplier for the initial value
     * @param reducer the function to reduce a previous output of reduce (or the initial value supplied)
     * with a current source value.
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> reduce(Supplier<R> initialSupplier, BiFunction<R, T, R> reducer) {
        Objects.requireNonNull(initialSupplier, "initialSupplier");
        Objects.requireNonNull(reducer, "reducer");
        // FIXME is there a reasonable ordered output of this?
        return new ParallelReduce<>(this, initialSupplier, reducer);
    }
    
    /**
     * Merges the values from each 'rail' in a round-robin or same-order fashion and
     * exposes it as a regular Publisher sequence, running with a default prefetch value
     * for the rails.
     * <p>
     * This operator uses the default prefetch size returned by {@code Px.bufferSize()}.
     * @return the new Px instance
     * @see ParallelPublisher#join(int)
     * @see ParallelPublisher#sequential()
     * @see ParallelPublisher#sequential(int)
     */
    public final Px<T> join() {
        return join(Px.bufferSize());
    }

    /**
     * Merges the values from each 'rail' in a round-robin or same-order fashion and
     * exposes it as a regular Publisher sequence, running with a give prefetch value
     * for the rails.
     * @param prefetch the prefetch amount to use for each rail
     * @return the new Px instance
     * @see ParallelPublisher#join()
     * @see ParallelPublisher#sequential()
     * @see ParallelPublisher#sequential(int)
     */
    public final Px<T> join(int prefetch) {
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        if (isOrdered()) {
            return new ParallelOrderedJoin<>((ParallelOrderedBase<T>)this, prefetch, Px.defaultQueueSupplier(prefetch));
        }
        return new ParallelUnorderedJoin<>(this, prefetch, Px.defaultQueueSupplier(prefetch));
    }
    
    /**
     * Turns this ParallelPublisher back into a sequential Publisher by merging
     * the values from each 'rail' in a round-robin or same-order fashion and
     * exposes it as a regular Publisher sequence, running with a give prefetch value
     * for the rails.
     * <p>
     * This operator uses the default prefetch size returned by {@code Px.bufferSize()}.
     * @return the new Px instance
     * @see ParallelPublisher#join()
     * @see ParallelPublisher#sequential()
     * @see ParallelPublisher#sequential(int)
     */
    public final Px<T> sequential() {
        return join();
    }
    
    /**
     * Turns this ParallelPublisher back into a sequential Publisher by merging
     * the values from each 'rail' in a round-robin or same-order fashion and
     * exposes it as a regular Publisher sequence, running with a default prefetch value
     * for the rails.
     * @param prefetch the prefetch amount to use for each rail
     * @return the new Px instance
     * @see ParallelPublisher#join()
     * @see ParallelPublisher#sequential()
     * @see ParallelPublisher#sequential(int)
     */
    public final Px<T> sequential(int prefetch) {
        return join(prefetch);
    }

    /**
     * Sorts the 'rails' of this ParallelPublisher and returns a Publisher that sequentially
     * picks the smallest next value from the rails.
     * <p>
     * This operator requires a finite source ParallelPublisher.
     * 
     * @param comparator the comparator to use
     * @return the new Px instance
     */
    public final Px<T> sorted(Comparator<? super T> comparator) {
        return sorted(comparator, 16);
    }

    /**
     * Sorts the 'rails' of this ParallelPublisher and returns a Publisher that sequentially
     * picks the smallest next value from the rails.
     * <p>
     * This operator requires a finite source ParallelPublisher.
     * 
     * @param comparator the comparator to use
     * @param capacityHint the expected number of total elements
     * @return the new Px instance
     */
    public final Px<T> sorted(Comparator<? super T> comparator, int capacityHint) {
        int ch = capacityHint / parallelism() + 1;
        ParallelPublisher<List<T>> railReduced = reduce(() -> new ArrayList<>(ch), (a, b) -> { a.add(b); return a; });
        ParallelPublisher<List<T>> railSorted = railReduced.map(list -> { list.sort(comparator); return list; });
        
        Px<T> merged = new ParallelSortedJoin<>(railSorted, comparator);
        
        return merged;
    }
    
    /**
     * Sorts the 'rails' according to the comparator and returns a full sorted list as a Publisher.
     * <p>
     * This operator requires a finite source ParallelPublisher.
     * 
     * @param comparator the comparator to compare elements
     * @return the new Px instannce
     */
    public final Px<List<T>> toSortedList(Comparator<? super T> comparator) {
        return toSortedList(comparator, 16);
    }
    /**
     * Sorts the 'rails' according to the comparator and returns a full sorted list as a Publisher.
     * <p>
     * This operator requires a finite source ParallelPublisher.
     * 
     * @param comparator the comparator to compare elements
     * @param capacityHint the expected number of total elements
     * @return the new Px instannce
     */
    public final Px<List<T>> toSortedList(Comparator<? super T> comparator, int capacityHint) {
        int ch = capacityHint / parallelism() + 1;
        ParallelPublisher<List<T>> railReduced = reduce(() -> new ArrayList<>(ch), (a, b) -> { a.add(b); return a; });
        ParallelPublisher<List<T>> railSorted = railReduced.map(list -> { list.sort(comparator); return list; });

        Px<List<T>> merged = railSorted.reduce((a, b) -> {
            int n = a.size() + b.size();
            if (n == 0) {
                return new ArrayList<>();
            }
            List<T> both = new ArrayList<>(n);
            
            Iterator<T> at = a.iterator();
            Iterator<T> bt = b.iterator();
            
            T s1 = at.hasNext() ? at.next() : null;
            T s2 = bt.hasNext() ? bt.next() : null;
            
            while (s1 != null && s2 != null) {
                if (comparator.compare(s1, s2) < 0) { // s1 comes before s2
                    both.add(s1);
                    s1 = at.hasNext() ? at.next() : null;
                } else {
                    both.add(s2);
                    s2 = bt.hasNext() ? bt.next() : null;
                }
            }

            if (s1 != null) {
                both.add(s1);
                while (at.hasNext()) {
                    both.add(at.next());
                }
            } else 
            if (s2 != null) {
                both.add(s2);
                while (bt.hasNext()) {
                    both.add(bt.next());
                }
            }
            
            return both;
        });
        
        return merged;
    }

    /**
     * Call the specified consumer with the current element passing through any 'rail'.
     * 
     * @param onNext the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnNext(Consumer<? super T> onNext) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    onNext,
                    v -> { },
                    e -> { },
                    () -> { },
                    () -> { },
                    s -> { },
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                onNext,
                v -> { },
                e -> { },
                () -> { },
                () -> { },
                s -> { },
                r -> { },
                () -> { }
                );
    }

    /**
     * Call the specified consumer with the current element passing through any 'rail'
     * after it has been delivered to downstream within the rail.
     * 
     * @param onAfterNext the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doAfterNext(Consumer<? super T> onAfterNext) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    onAfterNext,
                    e -> { },
                    () -> { },
                    () -> { },
                    s -> { },
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                onAfterNext,
                e -> { },
                () -> { },
                () -> { },
                s -> { },
                r -> { },
                () -> { }
                );
    }

    /**
     * Call the specified consumer with the exception passing through any 'rail'.
     * 
     * @param onError the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnError(Consumer<Throwable> onError) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    onError,
                    () -> { },
                    () -> { },
                    s -> { },
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                onError,
                () -> { },
                () -> { },
                s -> { },
                r -> { },
                () -> { }
                );
    }

    /**
     * Run the specified runnable when a 'rail' completes.
     * 
     * @param onComplete the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnComplete(Runnable onComplete) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    e -> { },
                    onComplete,
                    () -> { },
                    s -> { },
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                e -> { },
                onComplete,
                () -> { },
                s -> { },
                r -> { },
                () -> { }
                );
    }

    /**
     * Run the specified runnable when a 'rail' completes or signals an error.
     * 
     * @param onAfterTerminate the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doAfterTerminated(Runnable onAfterTerminate) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    e -> { },
                    () -> { },
                    onAfterTerminate,
                    s -> { },
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                e -> { },
                () -> { },
                onAfterTerminate,
                s -> { },
                r -> { },
                () -> { }
                );
    }

    /**
     * Call the specified callback when a 'rail' receives a Subscription from its upstream.
     * 
     * @param onSubscribe the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnCancel(Consumer<? super Subscription> onSubscribe) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    e -> { },
                    () -> { },
                    () -> { },
                    onSubscribe,
                    r -> { },
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                e -> { },
                () -> { },
                () -> { },
                onSubscribe,
                r -> { },
                () -> { }
                );
    }
    
    /**
     * Call the specified consumer with the request amount if any rail receives a request.
     * 
     * @param onRequest the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnRequest(LongConsumer onRequest) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    e -> { },
                    () -> { },
                    () -> { },
                    s -> { },
                    onRequest,
                    () -> { }
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                e -> { },
                () -> { },
                () -> { },
                s -> { },
                onRequest,
                () -> { }
                );
    }
    
    /**
     * Run the specified runnable when a 'rail' receives a cancellation.
     * 
     * @param onCancel the callback
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> doOnCancel(Runnable onCancel) {
        if (isOrdered()) {
            return new ParallelOrderedPeek<>((ParallelOrderedBase<T>)this,
                    v -> { },
                    v -> { },
                    e -> { },
                    () -> { },
                    () -> { },
                    s -> { },
                    r -> { },
                    onCancel
                    );
        }
        return new ParallelUnorderedPeek<>(this,
                v -> { },
                v -> { },
                e -> { },
                () -> { },
                () -> { },
                s -> { },
                r -> { },
                onCancel
                );
    }
    
    /**
     * Collect the elements in each rail into a collection supplied via a collectionSupplier
     * and collected into with a collector action, emitting the collection at the end.
     * 
     * @param <C> the collection type
     * @param collectionSupplier the supplier of the collection in each rail
     * @param collector the collector, taking the per-rali collection and the current item
     * @return the new ParallelPublisher instance
     */
    public final <C> ParallelPublisher<C> collect(Supplier<C> collectionSupplier, BiConsumer<C, T> collector) {
        return new ParallelCollect<>(this, collectionSupplier, collector);
    }

    /**
     * Collect the elements in each rail into a Stream-based Collector instance supplying
     * the initial collection, the collector action and the terminal transform, emitting this
     * latter value.
     * 
     * @param <A> the accumulated intermedate type
     * @param <R> the result type
     * @param collector the collector instance supplying the initial collection, the collector
     * action and the terminal transform.
     * @return the new ParallelPublisher instance
     */
    public final <A, R> ParallelPublisher<R> collect(Collector<T, A, R> collector) {
        return new ParallelStreamCollect<>(this, collector);
    }
    
    /**
     * Exposes the 'rails' as individual GroupedPublisher instances, keyed by the rail index (zero based).
     * <p>
     * Each group can be consumed only once; requests and cancellation compose through. Note
     * that cancelling only one rail may result in undefined behavior.
     * 
     * @return the new Px instance
     */
    public final Px<GroupedPublisher<Integer, T>> groups() {
        return new ParallelGroup<>(this);
    }
    
    /**
     * Wraps multiple Publishers into a ParallelPublisher which runs them
     * in parallel and unordered.
     * 
     * @param <T> the value type
     * @param publishers the array of publishers
     * @return the new ParallelPublisher instance
     */
    @SafeVarargs
    public static <T> ParallelPublisher<T> from(Publisher<T>... publishers) {
        if (publishers.length == 0) {
            throw new IllegalArgumentException("Zero publishers not supported");
        }
        return new ParallelUnorderedFrom<>(publishers);
    }

    /**
     * Turns this Parallel sequence into an ordered sequence via local indexing,
     * if not already ordered.
     * 
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> ordered() {
        return ordered(false);
    }

    /**
     * Turns this Parallel sequence into an ordered sequence,
     * if not already ordered.
     * 
     * @param global should the indexing locak (per rail) or globar FIFO?
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> ordered(boolean global) {
        if (isOrdered()) {
            return this;
        }
        return new ParallelToOrdered<>(this, global);
    }
    
    /**
     * Removes any ordering information from this Parallel sequence,
     * if not already unordered.
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> unordered() {
        if (!isOrdered()) {
            return this;
        }
        return new ParallelToUnordered<>((ParallelOrderedBase<T>)this);
    }
    
    /**
     * Perform a fluent transformation to a value via a converter function which
     * receives this ParallelPublisher.
     * 
     * @param <U> the output value type
     * @param converter the converter function from ParallelPublisher to some type
     * @return the value returned by the converter function
     */
    public final <U> U as(Function<? super ParallelPublisher<T>, U> converter) {
        return converter.apply(this);
    }
    
    /**
     * Allows composing operators, in assembly time, on top of this ParallelPublisher
     * and returns another ParallelPublisher with composed features.
     * 
     * @param <U> the output value type
     * @param composer the composer function from ParallelPublisher (this) to another ParallelPublisher
     * @return the ParallelPublisher returned by the function
     */
    public final <U> ParallelPublisher<U> compose(Function<? super ParallelPublisher<T>, ParallelPublisher<U>> composer) {
        return as(composer);
    }
    
    /**
     * Generates and flattens Publishers on each 'rail'.
     * <p>
     * Errors are not delayed and uses unbounded concurrency along with default inner prefetch.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return flatMap(mapper, false, Integer.MAX_VALUE, Px.bufferSize());
    }

    /**
     * Generates and flattens Publishers on each 'rail', optionally delaying errors.
     * <p>
     * It uses unbounded concurrency along with default inner prefetch.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param delayError should the errors from the main and the inner sources delayed till everybody terminates?
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> flatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError) {
        return flatMap(mapper, delayError, Integer.MAX_VALUE, Px.bufferSize());
    }

    /**
     * Generates and flattens Publishers on each 'rail', optionally delaying errors 
     * and having a total number of simultaneous subscriptions to the inner Publishers.
     * <p>
     * It uses a default inner prefetch.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param delayError should the errors from the main and the inner sources delayed till everybody terminates?
     * @param maxConcurrency the maximum number of simultaneous subscriptions to the generated inner Publishers
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> flatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency) {
        return flatMap(mapper, delayError, maxConcurrency, Px.bufferSize());
    }

    /**
     * Generates and flattens Publishers on each 'rail', optionally delaying errors, 
     * having a total number of simultaneous subscriptions to the inner Publishers
     * and using the given prefetch amount for the inner Publishers.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param delayError should the errors from the main and the inner sources delayed till everybody terminates?
     * @param maxConcurrency the maximum number of simultaneous subscriptions to the generated inner Publishers
     * @param prefetch the number of items to prefetch from each inner Publisher
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> flatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, int maxConcurrency, int prefetch) {
        return new ParallelFlatMap<>(this, mapper, delayError, maxConcurrency, Px.defaultQueueSupplier(maxConcurrency), prefetch, Px.defaultQueueSupplier(prefetch));
    }

    /**
     * Generates and concatenates Publishers on each 'rail', signalling errors immediately 
     * and generating 2 publishers upfront.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * source and the inner Publishers (immediate, boundary, end)
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> concatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return concatMap(mapper, 2, ErrorMode.IMMEDIATE);
    }

    /**
     * Generates and concatenates Publishers on each 'rail', signalling errors immediately 
     * and using the given prefetch amount for generating Publishers upfront.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param prefetch the number of items to prefetch from each inner Publisher
     * source and the inner Publishers (immediate, boundary, end)
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> concatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper,
                    int prefetch) {
        return concatMap(mapper, prefetch, ErrorMode.IMMEDIATE);
    }

    /**
     * Generates and concatenates Publishers on each 'rail', optionally delaying errors 
     * and generating 2 publishers upfront.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param errorMode the error handling, i.e., when to report errors from the main
     * source and the inner Publishers (immediate, boundary, end)
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> concatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper,
                    ErrorMode errorMode) {
        return concatMap(mapper, 2, errorMode);
    }

    /**
     * Generates and concatenates Publishers on each 'rail', optionally delaying errors 
     * and using the given prefetch amount for generating Publishers upfront.
     * 
     * @param <R> the result type
     * @param mapper the function to map each rail's value into a Publisher
     * @param prefetch the number of items to prefetch from each inner Publisher
     * @param errorMode the error handling, i.e., when to report errors from the main
     * source and the inner Publishers (immediate, boundary, end)
     * @return the new ParallelPublisher instance
     */
    public final <R> ParallelPublisher<R> concatMap(
            Function<? super T, ? extends Publisher<? extends R>> mapper,
                    int prefetch, ErrorMode errorMode) {
        if (isOrdered()) {
            return new ParallelOrderedConcatMap<>((ParallelOrderedBase<T>)this, mapper, Px.defaultQueueSupplier(prefetch), prefetch, errorMode);
        }
        return new ParallelUnorderedConcatMap<>(this, mapper, Px.defaultQueueSupplier(prefetch), prefetch, errorMode);
    }
}
