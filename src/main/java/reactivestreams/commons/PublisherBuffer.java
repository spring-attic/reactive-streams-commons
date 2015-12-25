package reactivestreams.commons;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.BackpressureHelper;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Buffers a certain number of subsequent elements and emits the buffers.
 *
 * @param <T> the source value type
 * @param <C> the buffer collection type
 */
public final class PublisherBuffer<T, C extends Collection<? super T>> implements Publisher<C> {

    final Publisher<? extends T> source;
    
    final int size;
    
    final int skip;
    
    final Supplier<C> bufferSupplier;

    public PublisherBuffer(Publisher<? extends T> source, int size, Supplier<C> bufferSupplier) {
        this(source, size, size, bufferSupplier);
    }

    public PublisherBuffer(Publisher<? extends T> source, int size, int skip, Supplier<C> bufferSupplier) {
        if (size <= 0) {
            throw new IllegalArgumentException("size > 0 required but it was " + size);
        }
        
        if (skip <= 0) {
            throw new IllegalArgumentException("skip > 0 required but it was " + size);
        }
        
        this.source = Objects.requireNonNull(source, "source");
        this.size = size;
        this.skip = skip;
        this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super C> s) {
        if (size == skip) {
            source.subscribe(new PublisherBufferExactSubscriber<>(s, size, bufferSupplier));
        } else 
        if (skip > size) {
            source.subscribe(new PublisherBufferSkipSubscriber<>(s, size, skip, bufferSupplier));
        } else {
            source.subscribe(new PublisherBufferOverlappingSubscriber<>(s, size, skip, bufferSupplier));
        }
    }
    
    static final class PublisherBufferExactSubscriber<T, C extends Collection<? super T>>
    implements Subscriber<T>, Subscription {

        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        final int size;
        
        C buffer;
        
        Subscription s;

        boolean done;
        
        public PublisherBufferExactSubscriber(Subscriber<? super C> actual, int size, Supplier<C> bufferSupplier) {
            this.actual = actual;
            this.size = size;
            this.bufferSupplier = bufferSupplier;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                s.request(BackpressureHelper.multiplyCap(n, size));
            }
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            
            C b = buffer;
            if (b == null) {
                
                try {
                    b = bufferSupplier.get();
                } catch (Throwable e) {
                    cancel();
                    
                    onError(e);
                    return;
                }
                
                if (b == null) {
                    cancel();
                    
                    onError(new NullPointerException("The bufferSupplier returned a null buffer"));
                    return;
                }
                buffer = b;
            }
            
            b.add(t);
            
            if (b.size() == size) {
                buffer = null;
                actual.onNext(b);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            C b = buffer;
            
            if (b != null && !b.isEmpty()) {
                actual.onNext(b);
            }
            actual.onComplete();
        }
    }

    static final class PublisherBufferSkipSubscriber<T, C extends Collection<? super T>>
    extends AtomicBoolean
    implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = 8971848569243655286L;

        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        final int size;

        final int skip;
        
        C buffer;
        
        Subscription s;

        boolean done;

        long index;
        
        public PublisherBufferSkipSubscriber(Subscriber<? super C> actual, int size, int skip,
                Supplier<C> bufferSupplier) {
            this.actual = actual;
            this.size = size;
            this.skip = skip;
            this.bufferSupplier = bufferSupplier;
        }

        @Override
        public void request(long n) {
            if (!get() && compareAndSet(false, true)) {
                // n full buffers
                long u = BackpressureHelper.multiplyCap(n, size);
                // + (n - 1) gaps
                long v = BackpressureHelper.multiplyCap(skip - size, n - 1);
                
                s.request(BackpressureHelper.addCap(u, v));
            } else {
                // n full buffer + gap
                s.request(BackpressureHelper.multiplyCap(skip, n));
            }
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }

            C b = buffer;
            
            long i = index;
            
            if (i % skip == 0L) {
                try {
                    b = bufferSupplier.get();
                } catch (Throwable e) {
                    cancel();
                    
                    onError(e);
                    return;
                }
                
                if (b == null) {
                    cancel();
                    
                    onError(new NullPointerException("The bufferSupplier returned a null buffer"));
                    return;
                }

                buffer = b;
            }

            if (b != null) {
                b.add(t);
                if (b.size() == size) {
                    buffer = null;
                    actual.onNext(b);
                }
            }
            
            index = i + 1;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            
            done = true;
            buffer = null;
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            
            done = true;
            C b = buffer;
            buffer = null;
            
            if (b != null) {
                actual.onNext(b);
            }
            
            actual.onComplete();
        }
    }

    
    static final class PublisherBufferOverlappingSubscriber<T, C extends Collection<? super T>>
    extends AtomicLong
    implements Subscriber<T>, Subscription {
        /** */
        private static final long serialVersionUID = 8971848569243655286L;

        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        final int size;

        final int skip;
        
        final ArrayDeque<C> buffers;
        
        Subscription s;

        boolean done;

        long index;
        
        volatile boolean cancelled;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherBufferOverlappingSubscriber> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherBufferOverlappingSubscriber.class, "once");
        
        static final long COMPLETED_MASK = 0x8000_0000_0000_0000L;
        static final long REQUESTED_MASK = 0x7FFF_FFFF_FFFF_FFFFL;
        
        
        public PublisherBufferOverlappingSubscriber(Subscriber<? super C> actual, int size, int skip,
                Supplier<C> bufferSupplier) {
            this.actual = actual;
            this.size = size;
            this.skip = skip;
            this.bufferSupplier = bufferSupplier;
            this.buffers = new ArrayDeque<>();
        }

        @Override
        public void request(long n) {

            if (!SubscriptionHelper.validate(n)) {
                return;
            }
            
            for (;;) {
                long r = get();

                // extract the current request amount
                long r0 = r & REQUESTED_MASK;
            
                // preserve COMPLETED_MASK and calculate new requested amount
                long u = (r & COMPLETED_MASK) | BackpressureHelper.addCap(r0, n);
            
                if (compareAndSet(r, u)) {
                    // (complete, 0) -> (complete, n) transition then replay
                    if (r == COMPLETED_MASK) {
                        
                        drain(n | COMPLETED_MASK);
                        
                        return;
                    }
                    // (active, r) -> (active, r + n) transition then continue with requesting from upstream
                    break;
                }
            }
            
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                // (n - 1) skips
                long u = BackpressureHelper.multiplyCap(skip, n - 1);
                
                // + 1 full buffer
                long r = BackpressureHelper.addCap(size, u);
                s.request(r);
            } else {
                // n skips
                long r = BackpressureHelper.multiplyCap(skip, n);
                s.request(r);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }

            ArrayDeque<C> bs = buffers;
            
            long i = index;
            
            if (i % skip == 0L) {
                C b;
                
                try {
                    b = bufferSupplier.get();
                } catch (Throwable e) {
                    cancel();
                    
                    onError(e);
                    return;
                }
                
                if (b == null) {
                    cancel();
                    
                    onError(new NullPointerException("The bufferSupplier returned a null buffer"));
                    return;
                }
                
                bs.offer(b);
            }

            C b = bs.peek();
            
            if (b != null && b.size() + 1 == size) {
                bs.poll();
                
                b.add(t);
                
                actual.onNext(b);

                if (get() != Long.MAX_VALUE) {
                    decrementAndGet();
                }
            }
            
            for (C b0 : bs) {
                b0.add(t);
            }
            
            index = i + 1;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            
            done = true;
            buffers.clear();
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            
            done = true;

            final ArrayDeque<C> bs = buffers;
            final Subscriber<? super C> a = actual;

            if (bs.isEmpty()) {
                a.onComplete();
                return;
            }

            drain(get());

            if (bs.isEmpty()) {
                return;
            }
            
            for (;;) {
                long r = get();

                long u = r | COMPLETED_MASK;
                // (active, r) -> (complete, r) transition
                if (compareAndSet(r, u)) {
                    // if the requested amount was non-zero, drain the queue
                    if (r != 0) {
                        drain(u);
                    }
                    
                    return;
                }
            }
        }
        
        void drain(long n) {
            final ArrayDeque<C> bs = buffers;
            final Subscriber<? super C> a = actual;
            
            long e = n & COMPLETED_MASK;
            
            for (;;) {
                
                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    C b = bs.poll();
                    
                    if (b == null) {
                        a.onComplete();
                        return;
                    }
                    
                    a.onNext(b);
                    e++;
                }

                if (cancelled) {
                    return;
                }
                
                if (bs.isEmpty()) {
                    a.onComplete();
                    return;
                }

                n = get();
                
                if (n == e) {
                    
                    n = addAndGet(-(e & REQUESTED_MASK));
                    
                    if ((n & REQUESTED_MASK) == 0L) {
                        return;
                    }
                    
                    e = n & COMPLETED_MASK;
                }
            }
        }
    }
}
