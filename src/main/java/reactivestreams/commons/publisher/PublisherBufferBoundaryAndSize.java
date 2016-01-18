package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.subscription.DeferredSubscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.*;

/**
 * Buffers elements into custom collections where the buffer boundary is signalled
 * by another publisher or the buffer reaches a size limit.
 *
 * @param <T> the source value type
 * @param <U> the element type of the boundary publisher (irrelevant)
 * @param <C> the output collection type
 */
public final class PublisherBufferBoundaryAndSize<T, U, C extends Collection<? super T>> 
extends PublisherSource<T, C> {

    final Publisher<U> other;
    
    final Supplier<C> bufferSupplier;
    
    final int maxSize;
    
    final Supplier<? extends Queue<C>> queueSupplier;

    public PublisherBufferBoundaryAndSize(Publisher<? extends T> source, 
            Publisher<U> other, Supplier<C> bufferSupplier, int maxSize,
            Supplier<? extends Queue<C>> queueSupplier) {
        super(source);
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize > 0 required but it was " + maxSize);
        }
        this.other = Objects.requireNonNull(other, "other");
        this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
        this.maxSize = maxSize;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super C> s) {
        C buffer;
        
        try {
            buffer = bufferSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (buffer == null) {
            EmptySubscription.error(s, new NullPointerException("The bufferSupplier returned a null buffer"));
            return;
        }
        
        Queue<C> q;
        
        try {
            q = queueSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
            return;
        }
        
        PublisherBufferBoundaryAndSizeMain<T, U, C> parent = new PublisherBufferBoundaryAndSizeMain<>(
                s, buffer, bufferSupplier, maxSize, q);
        
        PublisherBufferBoundaryAndSizeOther<U> boundary = new PublisherBufferBoundaryAndSizeOther<>(parent);
        parent.other = boundary;
        
        s.onSubscribe(parent);
        
        other.subscribe(boundary);
        
        source.subscribe(parent);
    }
    
    static final class PublisherBufferBoundaryAndSizeMain<T, U, C extends Collection<? super T>>
    implements Subscriber<T>, Subscription {

        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        PublisherBufferBoundaryAndSizeOther<U> other;
        
        C buffer;
        
        final Queue<C> queue;
        
        final int maxSize;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherBufferBoundaryAndSizeMain, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherBufferBoundaryAndSizeMain.class, Subscription.class, "s");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherBufferBoundaryAndSizeMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherBufferBoundaryAndSizeMain.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherBufferBoundaryAndSizeMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherBufferBoundaryAndSizeMain.class, "wip");
        
        volatile boolean done;
        Throwable error;
        
        volatile boolean cancelled;
        
        public PublisherBufferBoundaryAndSizeMain(Subscriber<? super C> actual, C buffer, 
                Supplier<C> bufferSupplier, int maxSize, Queue<C> queue) {
            this.actual = actual;
            this.buffer = buffer;
            this.bufferSupplier = bufferSupplier;
            this.queue = queue;
            this.maxSize = maxSize;
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
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                cancelMain();
                other.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            Throwable e = null;
            synchronized (this) {
                C b = buffer;
                if (b != null) {
                    b.add(t);
                    if (b.size() == maxSize) {
                        queue.offer(b);
                        
                        try {
                            b = bufferSupplier.get();
                            
                            if (b == null) {
                                e = new NullPointerException("The bufferSupplier returned a null value");
                            } else {
                                buffer = b;
                                return;
                            }
                        } catch (Throwable ex) {
                            e = ex;
                        }
                    } else {
                        return;
                    }
                }
            }
            UnsignalledExceptions.onNextDropped(t);
            
            if (e != null) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            other.cancel();

            boolean report;
            synchronized (this) {
                if (buffer == null) {
                    report = true;
                } else {
                    buffer = null;
                    report = false;
                }
            }
            
            if (report) {
                UnsignalledExceptions.onErrorDropped(t);
            } else {
                error = t;
                done = true;
                drain();
            }
        }

        @Override
        public void onComplete() {
            other.cancel();

            C b;
            synchronized (this) {
                b = buffer;
                if (b == null) {
                    return;
                }
                
                buffer = null;
                queue.offer(b);
            }

            done = true;
            drain();
        }
        
        void otherNext() {
            C c;
            
            try {
                c = bufferSupplier.get();
            } catch (Throwable e) {
                other.cancel();
                
                otherError(e);
                return;
            }
            
            if (c == null) {
                other.cancel();

                otherError(new NullPointerException("The bufferSupplier returned a null buffer"));
                return;
            }
            
            C b;
            synchronized (this) {
                b = buffer;
                if (b == null) {
                    return;
                }
                buffer = c;
                
                queue.offer(b);
            }
            drain();
        }
        
        void otherError(Throwable e) {
            cancelMain();
            
            boolean report;
            synchronized (this) {
                if (buffer == null) {
                    report = true;
                } else {
                    buffer = null;
                    report = false;
                }
            }
            
            if (report) {
                UnsignalledExceptions.onErrorDropped(e);
            } else {
                error = e;
                done = true;
                drain();
            }
        }
        
        void otherComplete() {
            
            cancelMain();
            
            C b;
            synchronized (this) {
                b = buffer;
                if (b == null) {
                    return;
                }
                
                buffer = null;
                queue.offer(b);
            }

            done = true;
            drain();
        }

        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            final Queue<C> q = queue;
            final Subscriber<? super C> a = actual;

            for (;;) {
                
                for (;;) {
                    boolean d = done;
                    
                    C b = q.poll();
                    
                    boolean empty = b == null;
                    
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    if (d) {
                        Throwable e = error;
                        if (e != null) {
                            q.clear();
                            
                            a.onError(e);
                            return;
                        } else
                        if (empty) {
                            a.onComplete();
                            return;
                        }
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    long r = requested;
                    if (r != 0L) {
                        a.onNext(b);
                        if (r != Long.MAX_VALUE) {
                            REQUESTED.decrementAndGet(this);
                        }
                    } else {
                        cancel();
                        q.clear();
                        
                        a.onError(new IllegalStateException("Could not emit value due to lack of requests"));
                        return;
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
    
    static final class PublisherBufferBoundaryAndSizeOther<U> extends DeferredSubscription
    implements Subscriber<U> {
        
        final PublisherBufferBoundaryAndSizeMain<?, U, ?> main;
        
        public PublisherBufferBoundaryAndSizeOther(PublisherBufferBoundaryAndSizeMain<?, U, ?> main) {
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
            main.otherNext();
        }
        
        @Override
        public void onError(Throwable t) {
            main.otherError(t);
        }
        
        @Override
        public void onComplete() {
            main.otherComplete();
        }
    }
}
