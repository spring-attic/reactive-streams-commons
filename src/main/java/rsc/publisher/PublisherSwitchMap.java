package rsc.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.util.BackpressureHelper;
import rsc.subscriber.CancelledSubscription;
import rsc.subscriber.EmptySubscription;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an item.
 * 
 * @param <T> the source value type
 * @param <R> the output value type
 */
public final class PublisherSwitchMap<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final Supplier<? extends Queue<Object>> queueSupplier;
    
    final int bufferSize;
    
    static final PublisherSwitchMapInner<Object> CANCELLED_INNER = new PublisherSwitchMapInner<>(null, 0, Long.MAX_VALUE);
    
    public PublisherSwitchMap(Publisher<? extends T> source, 
            Function<? super T, ? extends Publisher<? extends R>> mapper,
                    Supplier<? extends Queue<Object>> queueSupplier, int bufferSize) {
        super(source);
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
        }
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        
        if (PublisherFlatMap.trySubscribeScalarMap(source, s, mapper, false)) {
            return;
        }
        
        Queue<Object> q;
        
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
        
        source.subscribe(new PublisherSwitchMapMain<>(s, mapper, q, bufferSize));
    }
    
    static final class PublisherSwitchMapMain<T, R> implements Subscriber<T>, Subscription {
        
        final Subscriber<? super R> actual;
        
        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final Queue<Object> queue;
        
        final int bufferSize;

        Subscription s;
        
        volatile boolean done;

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherSwitchMapMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherSwitchMapMain.class, Throwable.class, "error");
        
        volatile boolean cancelled;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSwitchMapMain> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSwitchMapMain.class, "once");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherSwitchMapMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherSwitchMapMain.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSwitchMapMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSwitchMapMain.class, "wip");
        
        volatile PublisherSwitchMapInner<R> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherSwitchMapMain, PublisherSwitchMapInner> INNER =
                AtomicReferenceFieldUpdater.newUpdater(PublisherSwitchMapMain.class, PublisherSwitchMapInner.class, "inner");

        volatile long index;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherSwitchMapMain> INDEX =
                AtomicLongFieldUpdater.newUpdater(PublisherSwitchMapMain.class, "index");

        volatile int active;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSwitchMapMain> ACTIVE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSwitchMapMain.class, "active");

        
        public PublisherSwitchMapMain(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper, Queue<Object> queue, int bufferSize) {
            this.actual = actual;
            this.mapper = mapper;
            this.queue = queue;
            this.bufferSize = bufferSize;
            this.active = 1;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            long idx = INDEX.incrementAndGet(this);
            
            PublisherSwitchMapInner<R> si = inner;
            if (si != null) {
                si.deactivate();
                si.cancel();
            }
            
            Publisher<? extends R> p;
            
            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return;
            }
            
            if (p == null) {
                s.cancel();
                onError(new NullPointerException("The mapper returned a null publisher"));
                return;
            }
            
            PublisherSwitchMapInner<R> innerSubscriber = new PublisherSwitchMapInner<>(this, bufferSize, idx);
            
            if (INNER.compareAndSet(this, si, innerSubscriber)) {
                ACTIVE.getAndIncrement(this);
                p.subscribe(innerSubscriber);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                
                if (ONCE.compareAndSet(this, 0, 1)) {
                    deactivate();
                }
                
                cancelInner();
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            
            if (ONCE.compareAndSet(this, 0, 1)) {
                deactivate();
            }

            done = true;
            drain();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
                drain();
            }
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                if (WIP.getAndIncrement(this) == 0) {
                    cancelAndCleanup(queue);
                }
            }
        }
        
        void deactivate() {
            ACTIVE.decrementAndGet(this);
        }

        void cancelInner() {
            PublisherSwitchMapInner<?> si = INNER.getAndSet(this, CANCELLED_INNER);
            if (si != null && si != CANCELLED_INNER) {
                si.cancel();
                si.deactivate();
            }
        }
        
        void cancelAndCleanup(Queue<?> q) {
            s.cancel();
            
            cancelInner();
            
            q.clear();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            Subscriber<? super R> a = actual;
            Queue<Object> q = queue;
            
            int missed = 1;
            
            for (;;) {
                
                long r = requested;
                long e = 0L;
                
                while (r != e) {
                    boolean d = active == 0;
                    
                    @SuppressWarnings("unchecked")
                    PublisherSwitchMapInner<R> si = (PublisherSwitchMapInner<R>)q.poll();
                    
                    boolean empty = si == null;
                    
                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    Object second;
                    
                    while ((second = q.poll()) == null) ;
                    
                    if (index == si.index) {
                        
                        @SuppressWarnings("unchecked")
                        R v = (R)second;
                        
                        a.onNext(v);
                        
                        si.requestOne();
                        
                        e++;
                    }
                }
                
                if (r == e) {
                    if (checkTerminated(active == 0, q.isEmpty(), a, q)) {
                        return;
                    }
                }
                
                if (e != 0 && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                cancelAndCleanup(q);
                return true;
            }
            
            if (d) {
                Throwable e = ExceptionHelper.terminate(ERROR, this);
                if (e != null && e != ExceptionHelper.TERMINATED) {
                    cancelAndCleanup(q);
                    
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    a.onComplete();
                    return true;
                }
                
            }
            return false;
        }
        
        void innerNext(PublisherSwitchMapInner<R> inner, R value) {
            queue.offer(inner);
            queue.offer(value);
            drain();
        }
        
        void innerError(PublisherSwitchMapInner<R> inner, Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                s.cancel();
                
                if (ONCE.compareAndSet(this, 0, 1)) {
                    deactivate();
                }
                inner.deactivate();
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void innerComplete(PublisherSwitchMapInner<R> inner) {
            inner.deactivate();
            drain();
        }
    }
    
    static final class PublisherSwitchMapInner<R> implements Subscriber<R>, Subscription {
        
        final PublisherSwitchMapMain<?, R> parent;
        
        final int bufferSize;
        
        final int limit;
        
        final long index;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSwitchMapInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSwitchMapInner.class, "once");
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherSwitchMapInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherSwitchMapInner.class, Subscription.class, "s");
        
        int produced;
        
        public PublisherSwitchMapInner(PublisherSwitchMapMain<?, R> parent, int bufferSize, long index) {
            this.parent = parent;
            this.bufferSize = bufferSize;
            this.limit = bufferSize - (bufferSize >> 2);
            this.index = index;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            Subscription a = this.s;
            if (a == CancelledSubscription.INSTANCE) {
                s.cancel();
            }
            if (a != null) {
                s.cancel();
                
                SubscriptionHelper.reportSubscriptionSet();
                return;
            }
            
            if (S.compareAndSet(this, null, s)) {
                s.request(bufferSize);
                return;
            }
            a = this.s;
            if (a != CancelledSubscription.INSTANCE) {
                s.cancel();
                
                SubscriptionHelper.reportSubscriptionSet();
                return;
            }
        }
        
        @Override
        public void onNext(R t) {
            parent.innerNext(this, t);
        }
        
        @Override
        public void onError(Throwable t) {
            parent.innerError(this, t);
        }
        
        @Override
        public void onComplete() {
            parent.innerComplete(this);
        }
        
        void deactivate() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                parent.deactivate();
            }
        }
        
        void requestOne() {
            int p = produced + 1;
            if (p == limit) {
                produced = 0;
                s.request(p);
            } else {
                produced = p;
            }
        }
        
        @Override
        public void request(long n) {
            long p = produced + n;
            if (p >= limit) {
                produced = 0;
                s.request(p);
            } else {
                produced = (int)p;
            }
        }
        
        @Override
        public void cancel() {
            Subscription a = s;
            if (a != CancelledSubscription.INSTANCE) {
                a = S.getAndSet(this, CancelledSubscription.INSTANCE);
                if (a != null && a != CancelledSubscription.INSTANCE) {
                    a.cancel();
                }
            }
        }
    }
}
