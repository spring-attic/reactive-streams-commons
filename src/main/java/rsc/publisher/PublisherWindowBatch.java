package rsc.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.processor.UnicastProcessor;
import rsc.util.BackpressureHelper;
import rsc.subscriber.EmptySubscription;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Batches the source sequence into continuous, non-overlapping windows where the length of the windows
 * is determined by a fresh boundary Publisher or a maximum elements in that window.
 *
 * @param <T> the source value type
 * @param <U> the window boundary type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, innerOutput = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(innerOutput = { FusionMode.ASYNC })
public final class PublisherWindowBatch<T, U> extends PublisherSource<T, Px<T>> {

    final Supplier<? extends Publisher<U>> boundarySupplier;
    
    final Supplier<? extends Queue<Object>> mainQueueSupplier;
    
    final Supplier<? extends Queue<T>> windowQueueSupplier;
    
    final int maxSize;

    public PublisherWindowBatch(Publisher<? extends T> source, Supplier<? extends Publisher<U>> boundarySupplier,
            Supplier<? extends Queue<Object>> mainQueueSupplier, Supplier<? extends Queue<T>> windowQueueSupplier,
            int maxSize) {
        super(source);
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize > 0 required but it was " + maxSize);
        }
        this.boundarySupplier = Objects.requireNonNull(boundarySupplier, "boundarySupplier");
        this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
        this.windowQueueSupplier = Objects.requireNonNull(windowQueueSupplier, "windowQueueSupplier");
        this.maxSize = maxSize;
    }
    
    @Override
    public void subscribe(Subscriber<? super Px<T>> s) {
        Queue<Object> q;

        try {
            q = mainQueueSupplier.get();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The mainQueueSupplier returned a null queue"));
            return;
        }
        
        PublisherWindowBatchMain<T, U> parent = new PublisherWindowBatchMain<>(s, windowQueueSupplier, boundarySupplier, maxSize, q);
        
        source.subscribe(parent);
    }
    
    static final class PublisherWindowBatchMain<T, U> implements Subscriber<T>, Subscription, Runnable {

        final Subscriber<? super Px<T>> actual;

        final Supplier<? extends Queue<T>> windowQueueSupplier;

        final Supplier<? extends Publisher<U>> boundarySupplier;

        final int maxSize;
        
        final Queue<Object> queue;
        
        int size;
        
        Subscription s;
        
        volatile long index;
        
        UnicastProcessor<T> window;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowBatchMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowBatchMain.class, Throwable.class, "error");

        volatile PublisherWindowBatchOther<T, U> other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowBatchMain, PublisherWindowBatchOther> OTHER =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowBatchMain.class, PublisherWindowBatchOther.class, "other");
        
        static final Object DONE = new Object();
        
        volatile int open;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBatchMain> OPEN =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBatchMain.class, "open");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBatchMain> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBatchMain.class, "once");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherWindowBatchMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherWindowBatchMain.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBatchMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBatchMain.class, "wip");
        
        static final PublisherWindowBatchOther<Object, Object> OTHER_TERMINATED = new PublisherWindowBatchOther<>(null, Long.MAX_VALUE);
        
        public PublisherWindowBatchMain(Subscriber<? super Px<T>> actual,
                Supplier<? extends Queue<T>> windowQueueSupplier, Supplier<? extends Publisher<U>> boundarySupplier,
                int maxSize, Queue<Object> queue) {
            this.actual = actual;
            this.windowQueueSupplier = windowQueueSupplier;
            this.boundarySupplier = boundarySupplier;
            this.maxSize = maxSize;
            this.queue = queue;
            this.open = 1;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
                if (createWindow()) {
                    s.request(Long.MAX_VALUE);
                }
            }
        }

        boolean createWindow() {
            Queue<T> wq;
            
            try {
                wq = windowQueueSupplier.get();
            } catch (Throwable ex) {
                s.cancel();
                actual.onError(ex);
                return false;
            }
            
            UnicastProcessor<T> w = new UnicastProcessor<>(wq, this);
            window = w;
        
            Publisher<U> pu;
            
            try {
                pu = boundarySupplier.get();
            } catch (Throwable ex) {
                s.cancel();
                actual.onError(ex);
                return false;
            }
            
            PublisherWindowBatchOther<T, U> so = new PublisherWindowBatchOther<>(this, ++index);

            PublisherWindowBatchOther<T, U> prev = other;
            if (prev == OTHER_TERMINATED || !OTHER.compareAndSet(this, prev, so)) {
                return false;
            }
            
            OPEN.getAndIncrement(this);
            
            pu.subscribe(so);
            
            return true;
        }
         
        @Override
        public void onNext(T t) {
            synchronized (this) {
                queue.offer(t);
            }
            drain();
        }
        
        @Override
        public void onError(Throwable t) {
            cancelOther();
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            cancelOther();
            synchronized (this) {
                queue.offer(DONE);
            }
            drain();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
            }
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                if (OPEN.decrementAndGet(this) == 0) {
                    s.cancel();
                    cancelOther();
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        void cancelOther() {
            PublisherWindowBatchOther<T, U> a = other;
            if (a != OTHER_TERMINATED) {
                a = OTHER.getAndSet(this, OTHER_TERMINATED);
                if (a != null && a != OTHER_TERMINATED) {
                    a.cancel();
                }
            }
        }
        
        void otherNext(long index) {
            if (index == this.index) {
                WindowBoundary wb = new WindowBoundary(index);
                synchronized (this) {
                    queue.offer(wb);
                }
                drain();
            }
        }
        
        void otherError(long index, Throwable ex) {
            if (index == this.index) {
                s.cancel();
                cancelOther();
                if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                    drain();
                } else {
                    UnsignalledExceptions.onErrorDropped(ex);
                }
            } else {
                UnsignalledExceptions.onErrorDropped(ex);
            }
        }
        
        void otherComplete(long index) {
            if (index == this.index) {
                s.cancel();
                cancelOther();
                synchronized (this) {
                    queue.offer(DONE);
                }
                drain();
            }
        }
        
        static final class WindowBoundary {
            final long index;
            public WindowBoundary(long index) {
                this.index = index;
            }
        }
        
        @Override
        public void run() {
            if (OPEN.decrementAndGet(this) == 0) {
                s.cancel();
                cancelOther();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            Subscriber<? super Px<T>> a = actual;
            Queue<Object> q = queue;
            
            UnicastProcessor<T> w = window;
            
            int missed = 1;
            
            for (;;) {
                
                for (;;) {
                    Throwable ex = error;
                    if (ex != null) {
                        ex = ExceptionHelper.terminate(ERROR, this);
                        if (ex != ExceptionHelper.TERMINATED) {
                            q.clear();
                            
                            a.onError(ex);
                        }
                        return;
                    }
                    
                    Object o = q.poll();
                    
                    if (o == null) {
                        break;
                    }
                    
                    if (o == DONE) {
                        w.onComplete();
                        
                        a.onComplete();
                        return;
                    } else
                    if (o instanceof WindowBoundary) {
                        WindowBoundary wb = (WindowBoundary) o;
                        if (wb.index == index) {
                            size = 0;
                            w.onComplete();

                            if (once == 0) {
                                if (!createWindow()) {
                                    return;
                                }
                                w = window;
                            }
                        }
                    } else {
                        @SuppressWarnings("unchecked")
                        T v = (T)o;
                        w.onNext(v);
                        
                        int count = size + 1;
                        if (count == 1) {
                            if (requested != 0L) {
                                actual.onNext(w);
                                if (requested != Long.MAX_VALUE) {
                                    REQUESTED.decrementAndGet(this);
                                }
                            } else {
                                s.cancel();
                                cancelOther();
                                
                                actual.onError(new IllegalStateException("Could not emit initial window due to lack of requests"));
                                return;
                            }
                        }
                        if (count == maxSize) {
                            size = 0;
                            w.onComplete();
                            other.cancel();
                            if (once == 0) {
                                if (!createWindow()) {
                                    return;
                                }
                                w = window;
                            }
                        } else {
                            size = count;
                        }
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        static final class PublisherWindowBatchOther<T, U> implements Subscriber<U>, Subscription {
            
            final PublisherWindowBatchMain<T, U> parent;
            
            final long index;
            
            volatile Subscription s;
            @SuppressWarnings("rawtypes")
            static final AtomicReferenceFieldUpdater<PublisherWindowBatchOther, Subscription> S =
                    AtomicReferenceFieldUpdater.newUpdater(PublisherWindowBatchOther.class, Subscription.class, "s");
            
            public PublisherWindowBatchOther(PublisherWindowBatchMain<T, U> parent, long index) {
                this.parent = parent;
                this.index = index;
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (SubscriptionHelper.setOnce(S, this, s)) {
                    s.request(Long.MAX_VALUE);
                }
            }
            
            @Override
            public void onNext(U t) {
                cancel();
                parent.otherNext(index);
            }
            
            @Override
            public void onError(Throwable t) {
                parent.otherError(index, t);
            }
            
            @Override
            public void onComplete() {
                parent.otherComplete(index);
            }
            
            @Override
            public void cancel() {
                SubscriptionHelper.terminate(S, this);
            }
            
            @Override
            public void request(long n) {
                // ignored
            }
        }
    }
}
