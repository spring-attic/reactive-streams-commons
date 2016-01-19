package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.error.ExceptionHelper;
import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.subscription.DeferredSubscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.BackpressureHelper;
import reactivestreams.commons.support.SubscriptionHelper;

/**
 * Splits the source sequence into continuous, non-overlapping windowEnds 
 * where the window boundary is signalled by another Publisher
 * 
 * @param <T> the input value type
 * @param <U> the boundary publisher's type (irrelevant)
 */
public final class PublisherWindowBoundaryAndSize<T, U> extends PublisherSource<T, PublisherBase<T>> {

    final Publisher<U> other;
    
    final Supplier<? extends Queue<T>> processorQueueSupplier;

    final Supplier<? extends Queue<Object>> drainQueueSupplier;
    
    final int maxSize;

    public PublisherWindowBoundaryAndSize(Publisher<? extends T> source, Publisher<U> other, 
            Supplier<? extends Queue<T>> processorQueueSupplier,
                    Supplier<? extends Queue<Object>> drainQueueSupplier,
                            int maxSize) {
        super(source);
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize > 0 required but it was " + maxSize);
        }
        this.other = Objects.requireNonNull(other, "other");
        this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
        this.drainQueueSupplier = Objects.requireNonNull(drainQueueSupplier, "drainQueueSupplier");
        this.maxSize = maxSize;
    }

    @Override
    public void subscribe(Subscriber<? super PublisherBase<T>> s) {

        Queue<T> q;
        
        try {
            q = processorQueueSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The processorQueueSupplier returned a null queue"));
            return;
        }
        
        Queue<Object> dq;
        
        try {
            dq = drainQueueSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        if (dq == null) {
            EmptySubscription.error(s, new NullPointerException("The drainQueueSupplier returned a null queue"));
            return;
        }

        PublisherWindowBoundaryMain<T, U> main = new PublisherWindowBoundaryMain<>(s, processorQueueSupplier, q, dq, maxSize);
        
        s.onSubscribe(main);
        
        if (main.emit(main.window)) {
            other.subscribe(main.boundary);
            
            source.subscribe(main);
        }
    }
    
    static final class PublisherWindowBoundaryMain<T, U>
    implements Subscriber<T>, Subscription, Runnable {
        
        final Subscriber<? super PublisherBase<T>> actual;

        final Supplier<? extends Queue<T>> processorQueueSupplier;
        
        final PublisherWindowBoundaryOther<U> boundary;
        
        final Queue<Object> queue;
        
        final int maxSize;
        
        UnicastProcessor<T> window;
        
        int size;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowBoundaryMain, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, Subscription.class, "s");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherWindowBoundaryMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBoundaryMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, "wip");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowBoundaryMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, Throwable.class, "error");
        
        volatile boolean cancelled;

        volatile int open;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBoundaryMain> OPEN =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, "open");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowBoundaryMain> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowBoundaryMain.class, "once");

        static final Object BOUNDARY_MARKER = new Object();
        
        public PublisherWindowBoundaryMain(Subscriber<? super PublisherBase<T>> actual, 
                Supplier<? extends Queue<T>> processorQueueSupplier, 
                        Queue<T> processorQueue, Queue<Object> queue, int maxSize) {
            this.actual = actual;
            this.processorQueueSupplier = processorQueueSupplier;
            this.window = new UnicastProcessor<>(processorQueue, this);
            this.open = 2;
            this.boundary = new PublisherWindowBoundaryOther<>(this);
            this.queue = queue;
            this.maxSize = maxSize;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
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
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                mainDone();
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            synchronized (this) {
                queue.offer(BOUNDARY_MARKER);
            }
            mainDone();
            drain();
        }
        
        @Override
        public void run() {
            if (OPEN.decrementAndGet(this) == 0) {
                cancelMain();
                boundary.cancel();
            }
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
            }
        }
        
        void cancelMain() {
            SubscriptionHelper.terminate(S, this);
        }

        void mainDone() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                run();
            }
        }
        
        @Override
        public void cancel() {
            cancelled = true;
            mainDone();
        }
        
        void boundaryNext() {
            synchronized (this) {
                queue.offer(BOUNDARY_MARKER);
            }
            
            if (cancelled) {
                boundary.cancel();
            }
            
            drain();
        }
        
        void boundaryError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                mainDone();
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void boundaryComplete() {
            synchronized (this) {
                queue.offer(BOUNDARY_MARKER);
            }
            mainDone();
            drain();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            final Subscriber<? super PublisherBase<T>> a = actual;
            final Queue<Object> q = queue;
            UnicastProcessor<T> w = window;
            
            int missed = 1;
            
            for (;;) {
                
                for (;;) {
                    boolean d = open == 0 || error != null;
                    
                    Object o = q.poll();
                    
                    boolean empty = o == null;
                    
                    if (checkTerminated(d, empty, a, q, w)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    if (o != BOUNDARY_MARKER) {
                        @SuppressWarnings("unchecked")
                        T t = (T)o;
                        w.onNext(t);
                        
                        int localSize = size + 1;
                        if (localSize == maxSize) {
                            size = 0;
                            o = BOUNDARY_MARKER;
                        } else {
                            size = localSize;
                        }
                    }
                    
                    if (o == BOUNDARY_MARKER) {
                        window = null;

                        w.onComplete();
                        
                        if (!cancelled && open != 0 && error == null) {
                            
                            Queue<T> pq;

                            try {
                                pq = processorQueueSupplier.get();
                            } catch (Throwable e) {
                                emitError(a, e);
                                return;
                            }
                            
                            if (pq == null) {
                                emitError(a, new NullPointerException("The processorQueueSupplier returned a null queue"));
                                return;
                            }
                            
                            OPEN.getAndIncrement(this);
                            
                            w = new UnicastProcessor<>(pq, this);
                            
                            long r = requested;
                            if (r != 0L) {
                                size = 0;
                                window = w;

                                a.onNext(w);
                                if (r != Long.MAX_VALUE) {
                                    REQUESTED.decrementAndGet(this);
                                }
                            } else {
                                Throwable e = new IllegalStateException("Could not emit window due to lack of requests");

                                emitError(a, e);
                                return;
                            }
                        }
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void emitError(Subscriber<?> a, Throwable e) {
            cancelMain();
            boundary.cancel();
            
            ExceptionHelper.addThrowable(ERROR, this, e);
            e = ExceptionHelper.terminate(ERROR, this);
            
            a.onError(e);
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q, UnicastProcessor<?> w) {
            if (d) {
                Throwable e = ExceptionHelper.terminate(ERROR, this);
                if (e != null && e != ExceptionHelper.TERMINATED) {
                    cancelMain();
                    boundary.cancel();
                    
                    w.onError(e);
                    
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    cancelMain();
                    boundary.cancel();

                    w.onComplete();
                    
                    a.onComplete();
                    return true;
                }
            }
            
            return false;
        }
        
        boolean emit(UnicastProcessor<T> w) {
            long r = requested;
            if (r != 0L) {
                actual.onNext(w);
                if (r != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }
                return true;
            } else {
                cancel();
                
                actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));

                return false;
            }
        }
    }
    
    static final class PublisherWindowBoundaryOther<U>
            extends DeferredSubscription
    implements Subscriber<U> {
        
        final PublisherWindowBoundaryMain<?, U> main;

        public PublisherWindowBoundaryOther(PublisherWindowBoundaryMain<?, U> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(U t) {
            main.boundaryNext();
        }

        @Override
        public void onError(Throwable t) {
            main.boundaryError(t);
        }

        @Override
        public void onComplete() {
            main.boundaryComplete();
        }
    }
}
