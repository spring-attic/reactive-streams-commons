package rsc.parallel;

import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.publisher.Px;
import rsc.scheduler.Scheduler;

public abstract class ParallelPublisher<T> {
    
    public abstract void subscribe(Subscriber<? super T>[] subscribers);
    
    public abstract int parallelism();
    
    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source) {
        return fork(source, false, Runtime.getRuntime().availableProcessors(), Px.bufferSize(), Px.defaultQueueSupplier(Px.bufferSize()));
    }

    public static <T> ParallelPublisher<T> fork(Publisher<? extends T> source, boolean ordered, int parallelism, int prefetch, Supplier<Queue<Object>> queueSupplier) {
        throw new UnsupportedOperationException();
    }

    
    public <U> ParallelPublisher<U> map(Function<? super T, ? extends U> mapper) {
        throw new UnsupportedOperationException();
    }
    
    public ParallelPublisher<T> filter(Predicate<? super T> predicate) {
        throw new UnsupportedOperationException();
    }
    
    public ParallelPublisher<T> runOn(Scheduler scheduler) {
        throw new UnsupportedOperationException();
    }
    
    public Publisher<T> reduce(BiFunction<T, T, T> reducer) {
        throw new UnsupportedOperationException();
    }
    
    public <R> ParallelPublisher<R> reduce(Supplier<R> initialSupplier, BiFunction<R, T, R> reducer) {
        throw new UnsupportedOperationException();
    }
    
    public Publisher<T> join() {
        throw new UnsupportedOperationException();
    }
}
