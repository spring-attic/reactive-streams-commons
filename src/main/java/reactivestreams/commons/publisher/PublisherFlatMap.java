package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import reactivestreams.commons.util.*;
import reactor.core.util.Exceptions;

/**
 * Maps a sequence of values each into a Publisher and flattens them 
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
public final class PublisherFlatMap<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final boolean delayError;
    
    final int prefetch;
    
    final int maxConcurrency;
    
    final Supplier<? extends Queue<R>> queueSupplier;
    
    public PublisherFlatMap(Publisher<? extends T> source, Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, int maxConcurrency, Supplier<? extends Queue<R>> queueSupplier, int prefetch) {
        super(source);
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
        }
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.delayError = delayError;
        this.prefetch = prefetch;
        this.maxConcurrency = maxConcurrency;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        source.subscribe(new PublisherFlatMapMain<>(s, mapper, delayError, maxConcurrency, queueSupplier, prefetch));
    }

    static final class PublisherFlatMapMain<T, R> 
    implements Subscriber<T>, Subscription {
        
        final Subscriber<? super R> actual;

        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final boolean delayError;
        
        final int maxConcurrency;
        
        final Supplier<? extends Queue<R>> queueSupplier;

        final int prefetch;
        
        final int limit;
        
        volatile Queue<R> scalarQueue;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapMain.class, Throwable.class, "error");
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        Subscription s;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherFlatMapMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherFlatMapMain.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherFlatMapMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherFlatMapMain.class, "wip");
        
        volatile PublisherFlatMapInner<R>[] subscribers;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapMain, PublisherFlatMapInner[]> SUBSCRIBERS =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapMain.class, PublisherFlatMapInner[].class, "subscribers");
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] EMPTY = new PublisherFlatMapInner[0];
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] TERMINATED = new PublisherFlatMapInner[0];
        
        long index;

        int last;
        
        int produced;
        
        public PublisherFlatMapMain(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
                Supplier<? extends Queue<R>> queueSupplier, int prefetch) {
            this.actual = actual;
            this.mapper = mapper;
            this.delayError = delayError;
            this.maxConcurrency = maxConcurrency;
            this.queueSupplier = queueSupplier;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            SUBSCRIBERS.lazySet(this, EMPTY);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                if (WIP.getAndIncrement(this) == 0) {
                    scalarQueue = null;
                    s.cancel();
                    cancelAllInner();
                }
            }
        }

        @SuppressWarnings("unchecked")
        void cancelAllInner() {
            PublisherFlatMapInner<R>[] a = subscribers;
            if (a != TERMINATED) {
                a = SUBSCRIBERS.getAndSet(this, TERMINATED);
                if (a != TERMINATED) {
                    for (PublisherFlatMapInner<R> e : a) {
                        e.cancel();
                    }
                }
            }
        }
        
        boolean add(PublisherFlatMapInner<R> inner) {
            for (;;) {
                PublisherFlatMapInner<R>[] a = subscribers;
                if (a == TERMINATED) {
                    return false;
                }
                
                int n = a.length;
                
                @SuppressWarnings("unchecked")
                PublisherFlatMapInner<R>[] b = new PublisherFlatMapInner[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;
                
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return true;
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        void remove(PublisherFlatMapInner<R> inner) {
            for (;;) {
                PublisherFlatMapInner<R>[] a = subscribers;
                if (a == TERMINATED || a == EMPTY) {
                    return;
                }
                
                int n = a.length;

                int j = -1;
                
                for (int i = 0; i < n; i++) {
                    if (a[i] == inner) {
                        j = i;
                        break;
                    }
                }
                
                if (j < 0) {
                    return;
                }
                
                PublisherFlatMapInner<R>[] b;
                if (n == 1) {
                    b = EMPTY;
                } else {
                    b = new PublisherFlatMapInner[n - 1];
                }
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return;
                }
            }
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                if (maxConcurrency == Integer.MAX_VALUE) {
                    s.request(Long.MAX_VALUE);
                } else {
                    s.request(maxConcurrency);
                }
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            Publisher<? extends R> p;
            
            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                s.cancel();
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
            
            if (p == null) {
                s.cancel();

                onError(new NullPointerException("The mapper returned a null Publisher"));
                return;
            }
            
            if (p instanceof Supplier) {
                @SuppressWarnings("unchecked")
                R v = ((Supplier<R>)p).get();
                emitScalar(v);
            } else {
                PublisherFlatMapInner<R> inner = new PublisherFlatMapInner<>(this, prefetch, index++);
                if (add(inner)) {
                    
                    p.subscribe(inner);
                }
            }
            
        }
        
        void emitScalar(R v) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != 0L) {
                    actual.onNext(v);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    
                    if (maxConcurrency != Integer.MAX_VALUE) {
                        int p = produced + 1;
                        if (p == limit) {
                            produced = 0;
                            s.request(p);
                        } else {
                            produced = p;
                        }
                    }
                    
                    if (WIP.decrementAndGet(this) == 0) {
                        return;
                    }
                } else {
                    Queue<R> q = getOrCreateScalarQueue();
                    
                    if (!q.offer(v)) {
                        s.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                    }
                }
                drainLoop();
            } else {
                Queue<R> q = getOrCreateScalarQueue();
                
                if (!q.offer(v)) {
                    s.cancel();
                    
                    Throwable e = new IllegalStateException("Scalar queue full?!");
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                        done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(e);
                    }
                }
                drain();
            }
        }
        
        Queue<R> getOrCreateScalarQueue() {
            Queue<R> q = scalarQueue;
            if (q == null) {
                q = queueSupplier.get();
                scalarQueue = q;
            }
            return q;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
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
            
            done = true;
            drain();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) == 0) {
                return;
            }
            drainLoop();
        }
        
        void drainLoop() {
            int missed = 1;
            
            final Subscriber<? super R> a = actual;
            
            for (;;) {
                
                
                PublisherFlatMapInner<R>[] as = subscribers;
                
                int n = as.length;

                boolean d = done;
                Queue<R> sq = scalarQueue;
                
                if (checkTerminated(d, n == 0 && (sq == null || sq.isEmpty()), a)) {
                    return;
                }
                
                long r = requested;
                long e = 0L;
                long replenishMain = 0L;
                
                if (r != 0L) {
                    sq = scalarQueue;
                    if (sq != null) {
                        
                        while (e != r) {
                            d = done;
                            
                            R v = sq.poll();
                            
                            boolean empty = v == null;

                            if (checkTerminated(d, false, a)) {
                                return;
                            }
                            
                            if (empty) {
                                break;
                            }
                            
                            a.onNext(v);
                            
                            e++;
                        }
                        
                        if (e != 0L) {
                            replenishMain += e;
                            if (r != Long.MAX_VALUE) {
                                r = REQUESTED.addAndGet(this, -e);
                            }
                        }
                    }

                    if (r != 0L) {
                        // TODO
                        
                    }
                }
                
                
                if (replenishMain != 0L) {
                    s.request(replenishMain);
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
            if (cancelled) {
                scalarQueue = null;
                s.cancel();
                cancelAllInner();
                
                return true;
            }
            
            if (delayError) {
                if (done && empty) {
                    Throwable e = error;
                    if (e != null && e != Exceptions.TERMINATED) {
                        e = Exceptions.terminate(ERROR, this);
                        a.onError(e);
                    } else {
                        a.onComplete();
                    }
                    
                    return true;
                }
            } else {
                if (done) {
                    Throwable e = error;
                    if (e != null && e != Exceptions.TERMINATED) {
                        e = Exceptions.terminate(ERROR, this);
                        scalarQueue = null;
                        s.cancel();
                        cancelAllInner();
                        
                        a.onError(e);
                        return true;
                    } else 
                    if (empty) {
                        a.onComplete();
                    }
                }
            }
            
            return false;
        }
        
        void innerError(PublisherFlatMapInner<R> inner, Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                inner.done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void innerNext(PublisherFlatMapInner<R> inner, R v) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != 0L) {
                    actual.onNext(v);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    
                    inner.request(1);
                    
                    if (WIP.decrementAndGet(this) == 0) {
                        return;
                    }
                } else {
                    Queue<R> q = getOrCreateScalarQueue(inner);
                    
                    if (!q.offer(v)) {
                        inner.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            inner.done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                    }
                }
                drainLoop();
            } else {
                Queue<R> q = getOrCreateScalarQueue(inner);
                
                if (!q.offer(v)) {
                    inner.cancel();
                    
                    Throwable e = new IllegalStateException("Scalar queue full?!");
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                        inner.done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(e);
                    }
                }
                drain();
            }
        }
        
        Queue<R> getOrCreateScalarQueue(PublisherFlatMapInner<R> inner) {
            Queue<R> q = inner.queue;
            if (q == null) {
                q = queueSupplier.get();
                inner.queue = q;
            }
            return q;
        }
    }
    
    static final class PublisherFlatMapInner<R> 
    implements Subscriber<R>, Subscription {

        final PublisherFlatMapMain<?, R> parent;
        
        final int prefetch;
        
        final int limit;
        
        final long index;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapInner.class, Subscription.class, "s");
        
        long produced;
        
        volatile Queue<R> queue;
        
        volatile boolean done;
        
        boolean synchronousSource;
        
        public PublisherFlatMapInner(PublisherFlatMapMain<?, R> parent, int prefetch, long index) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.index = index;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                if (s instanceof SynchronousSource) {
                    synchronousSource = true;
                    queue = (SynchronousSource<R>)s;
                    done = true;
                    parent.drain();
                } else {
                    s.request(prefetch);
                }
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
            done = true;
            parent.drain();
        }

        @Override
        public void request(long n) {
            if (!synchronousSource) {
                long p = produced + 1;
                if (p == limit) {
                    produced = 0L;
                    s.request(p);
                } else {
                    produced = p;
                }
            }
        }

        @Override
        public void cancel() {
            SubscriptionHelper.terminate(S, this);
        }
        
    }
}
