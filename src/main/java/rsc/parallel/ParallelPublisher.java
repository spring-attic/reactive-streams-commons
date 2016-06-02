package rsc.parallel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.publisher.GroupedPublisher;
import rsc.publisher.Px;
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
    public abstract boolean ordered();
    
    /**
     * Validates the number of subscribers and returns true if their number
     * matches the parallelism level of this ParallelPublisher.
     * 
     * @param subscribers the array of Subscribers
     * @return true if the number of subscribers equals to the parallelism level
     */
    protected final boolean validate(Subscriber<? super T>[] subscribers) {
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
        if (ordered) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism > 0 required but it was " + parallelism);
        }
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        
        Objects.requireNonNull(queueSupplier, "queueSupplier");
        Objects.requireNonNull(source, "queueSupplier");
        
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
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(mapper, "mapper");
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
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(predicate, "predicate");
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
        return runOn(scheduler, false);
    }

    /**
     * Specifies where each 'rail' will observe its incoming values with
     * possibly work-stealing and default prefetch amount.
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
     * @param workStealing if true, values traveling on 'rails' may hop to another rail if
     * that rail's worker has run out of work.
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> runOn(Scheduler scheduler, boolean workStealing) {
        return runOn(scheduler, workStealing, Px.bufferSize());
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
     * @param workStealing if true, values traveling on 'rails' may hop to another rail if
     * that rail's worker has run out of work.
     * @param prefetch the number of values to request on each 'rail' from the source
     * @return the new ParallelPublisher instance
     */
    public final ParallelPublisher<T> runOn(Scheduler scheduler, boolean workStealing, int prefetch) {
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        if (workStealing) {
            throw new UnsupportedOperationException("workStealing not supported yet");
        }
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        Objects.requireNonNull(scheduler, "scheduler");
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
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        return new ParallelUnorderedReduceFull<>(this, reducer);
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
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(initialSupplier, "initialSupplier");
        Objects.requireNonNull(reducer, "reducer");
        return new ParallelUnorderedReduce<>(this, initialSupplier, reducer);
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
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
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
        return new ParallelUnorderedCollect<>(this, collectionSupplier, collector);
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
        return new ParallelUnorderedStreamCollect<>(this, collector);
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
        return new ParallelUnorderedGroup<>(this);
    }
}
