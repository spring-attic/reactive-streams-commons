package rsc.publisher;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.subscriber.CancelledSubscription;
import rsc.subscriber.SubscriptionHelper;

/**
 * Creates a Future that consumes a source Publisher and returns the very last value,
 * the Throwable or a default / NoSuchElementException if the source is empty. 
 *
 * @param <T>
 */
public final class BlockingFuture<T> {

    final Publisher<? extends T> source;

    public BlockingFuture(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }
    
    public Future<T> future() {
        BlockingFutureSubscriber<T> bfs = new BlockingFutureSubscriber<>(null);
        
        source.subscribe(bfs);
        
        return bfs;
    }
    
    public Future<T> future(T defaultValue) {
        Objects.requireNonNull(defaultValue, "defaultValue");
        
        BlockingFutureSubscriber<T> bfs = new BlockingFutureSubscriber<>(defaultValue);
        
        source.subscribe(bfs);
        
        return bfs;
    }
    
    public CompletableFuture<T> completableFuture() {
        BlockingCompletableFutureSubscriber<T> bfs = new BlockingCompletableFutureSubscriber<>(null);
        
        source.subscribe(bfs);
        
        return bfs;
    }

    public CompletableFuture<T> completableFuture(T defaultValue) {
        Objects.requireNonNull(defaultValue, "defaultValue");
        
        BlockingCompletableFutureSubscriber<T> bfs = new BlockingCompletableFutureSubscriber<>(defaultValue);
        
        source.subscribe(bfs);
        
        return bfs;
    }

    static final class BlockingFutureSubscriber<T> 
    implements Subscriber<T>, Future<T> {

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<BlockingFutureSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(BlockingFutureSubscriber.class, Subscription.class, "s");
        
        T value;
        Throwable error;
        
        final CountDownLatch cdl;
        
        public BlockingFutureSubscriber(T defaultValue) {
            this.value = defaultValue;
            this.cdl = new CountDownLatch(1);
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            value = t;
        }

        @Override
        public void onError(Throwable t) {
            value = null;
            error = t;
            cdl.countDown();
        }

        @Override
        public void onComplete() {
            cdl.countDown();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return SubscriptionHelper.terminate(S, this);
        }

        @Override
        public boolean isCancelled() {
            return s == CancelledSubscription.INSTANCE;
        }

        @Override
        public boolean isDone() {
            return cdl.getCount() == 0L;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            if (cdl.getCount() != 0) {
                cdl.await();
            }
            return emit();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (cdl.getCount() != 0) {
                if (!cdl.await(timeout, unit)) {
                    throw new TimeoutException();
                }
            }
            return emit();
        }
        
        T emit() throws ExecutionException {
            Throwable e = error;
            if (e != null) {
                throw new ExecutionException(e);
            }
            T v = value;
            if (v == null) {
                throw new ExecutionException(new NoSuchElementException());
            }
            return v;
        }
    }
    
    static final class BlockingCompletableFutureSubscriber<T>
    extends CompletableFuture<T>
    implements Subscriber<T> {

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<BlockingCompletableFutureSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(BlockingCompletableFutureSubscriber.class, Subscription.class, "s");
        
        T value;
        
        public BlockingCompletableFutureSubscriber(T defaultValue) {
            this.value = defaultValue;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            value = t;
        }

        @Override
        public void onError(Throwable t) {
            value = null;
            completeExceptionally(t);
        }

        @Override
        public void onComplete() {
            T v = value;
            value = null;
            if (v == null) {
                completeExceptionally(new NoSuchElementException());
            } else {
                complete(v);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            SubscriptionHelper.terminate(S, this);
            return super.cancel(mayInterruptIfRunning);
        }
    }

}
