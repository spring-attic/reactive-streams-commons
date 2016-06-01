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
    
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source) {
        return fork(source, false, Runtime.getRuntime().availableProcessors(), Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered) {
        return fork(source, ordered, Runtime.getRuntime().availableProcessors(), Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered, int parallelism) {
        return fork(source, ordered, parallelism, Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered, int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
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

    
    public final <U> ParallelPublisher<U> map(Function<? super T, ? extends U> mapper) {
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(mapper, "mapper");
        return new ParallelUnorderedMap<>(this, mapper);
    }
    
    public final ParallelPublisher<T> filter(Predicate<? super T> predicate) {
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(predicate, "predicate");
        return new ParallelUnorderedFilter<>(this, predicate);
    }
    
    public final ParallelPublisher<T> runOn(Scheduler scheduler) {
        return runOn(scheduler, false);
    }

    public final ParallelPublisher<T> runOn(Scheduler scheduler, boolean workStealing) {
        return runOn(scheduler, workStealing, Px.bufferSize());
    }

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

    public final Publisher<T> reduce(BiFunction<T, T, T> reducer) {
        Objects.requireNonNull(reducer, "reducer");
        throw new UnsupportedOperationException();
    }
    
    public final <R> ParallelPublisher<R> reduce(Supplier<R> initialSupplier, BiFunction<R, T, R> reducer) {
        if (ordered()) {
            throw new UnsupportedOperationException("ordered not supported yet");
        }
        Objects.requireNonNull(initialSupplier, "initialSupplier");
        Objects.requireNonNull(reducer, "reducer");
        throw new UnsupportedOperationException();
    }
    
    public final Px<T> join() {
        return join(Px.bufferSize());
    }

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
