package rsc.parallel;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

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
}
