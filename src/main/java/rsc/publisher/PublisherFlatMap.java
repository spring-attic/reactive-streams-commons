package rsc.publisher;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;

import rsc.subscriber.ScalarSubscription;
import rsc.subscriber.SubscriberState;
import rsc.subscriber.SubscriptionHelper;
import rsc.subscriber.SuppressFuseableSubscriber;
import rsc.util.*;

/**
 * Maps a sequence of values each into a Publisher and flattens them 
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, innerInput = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SCALAR }, innerInput = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.SCALAR })
public final class PublisherFlatMap<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final boolean delayError;
    
    final int maxConcurrency;
    
    final Supplier<? extends Queue<R>> mainQueueSupplier;

    final int prefetch;
    
    final Supplier<? extends Queue<R>> innerQueueSupplier;
    
    public PublisherFlatMap(Publisher<? extends T> source, Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
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
        this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
        this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
    }

    @Override
    public long getPrefetch() {
        return prefetch;
    }

    /**
     * Return a Subscriber that handles the merging operation with the given parameters.
     * @param <T> the input value type
     * @param <R> the output value type
     * @param s the downstream Subscriber
     * @param mapper the mapper from Ts to a Publisher of Rs
     * @param delayError delay the errors?
     * @param maxConcurrency maximum number of simultaneous subscriptions to the generated sources
     * @param mainQueueSupplier the supplier for the main queue
     * @param prefetch the prefetch amount for the inner sources
     * @param innerQueueSupplier the queue supplier for the inner sources
     * @return the Subscriber
     */
    public static <T, R> Subscriber<T> subscribe(
            Subscriber<? super R> s, 
            Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, 
            int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, 
            int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
        return new PublisherFlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier);
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        
        if (trySubscribeScalarMap(source, s, mapper, false)) {
            return;
        }
        
        source.subscribe(new PublisherFlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier));
    }

    /**
     * Checks if the source is a Supplier and if the mapper's publisher output is also
     * a supplier, thus avoiding subscribing to any of them.
     *
     * @param source the source publisher
     * @param s the end consumer
     * @param mapper the mapper function
     * @param fuseableExpected if true, the parent class was marked Fuseable thus the mapping
     * output has to signal onSubscribe with a QueueSubscription
     * @return true if the optimization worked
     */
    @SuppressWarnings("unchecked")
    static <T, R> boolean trySubscribeScalarMap(
            Publisher<? extends T> source,
            Subscriber<? super R> s,
            Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean fuseableExpected) {
        if (source instanceof Callable) {
            T t;

            try {
                t = ((Callable<? extends T>)source).call();
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                SubscriptionHelper.error(s, ExceptionHelper.unwrap(e));
                return true;
            }

            if (t == null) {
                SubscriptionHelper.complete(s);
                return true;
            }

            Publisher<? extends R> p;

            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                SubscriptionHelper.error(s, ExceptionHelper.unwrap(e));
                return true;
            }

            if (p == null) {
                SubscriptionHelper.error(s, new NullPointerException("The mapper returned a null Publisher"));
                return true;
            }

            if (p instanceof Callable) {
                R v;

                try {
                    v = ((Callable<R>)p).call();
                } catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    SubscriptionHelper.error(s, ExceptionHelper.unwrap(e));
                    return true;
                }

                if (v != null) {
                    s.onSubscribe(new ScalarSubscription<>(s, v));
                } else {
                    SubscriptionHelper.complete(s);
                }
            } else {
                if (!fuseableExpected || p instanceof Fuseable) {
                    p.subscribe(s);
                } else {
                    p.subscribe(new SuppressFuseableSubscriber<>(s));
                }
            }

            return true;
        }

        return false;
    }

    static final class PublisherFlatMapMain<T, R>
            extends SpscFreeListTracker<PublisherFlatMapInner<R>>
    implements Subscriber<T>, Subscription, Receiver, MultiReceiver, Producer,
               SubscriberState {
        
        final Subscriber<? super R> actual;

        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final boolean delayError;
        
        final int maxConcurrency;
        
        final Supplier<? extends Queue<R>> mainQueueSupplier;

        final int prefetch;

        final Supplier<? extends Queue<R>> innerQueueSupplier;
        
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
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] EMPTY = new PublisherFlatMapInner[0];
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] TERMINATED = new PublisherFlatMapInner[0];
        
        int lastIndex;
        
        int produced;
        
        public PublisherFlatMapMain(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
                Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
            this.actual = actual;
            this.mapper = mapper;
            this.delayError = delayError;
            this.maxConcurrency = maxConcurrency;
            this.mainQueueSupplier = mainQueueSupplier;
            this.prefetch = prefetch;
            this.innerQueueSupplier = innerQueueSupplier;
            this.limit = maxConcurrency - (maxConcurrency >> 2);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected PublisherFlatMapInner<R>[] empty() {
            return EMPTY;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected PublisherFlatMapInner<R>[] terminated() {
            return TERMINATED;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected PublisherFlatMapInner<R>[] newArray(int size) {
            return new PublisherFlatMapInner[size];
        }
        
        @Override
        protected void setIndex(PublisherFlatMapInner<R> entry, int index) {
            entry.index = index;
        }
        
        @Override
        protected void unsubscribeEntry(PublisherFlatMapInner<R> entry) {
            entry.cancel();
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
                    scalarQueue = null;
                    s.cancel();
                    unsubscribe();
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

        @SuppressWarnings("unchecked")
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
                ExceptionHelper.throwIfFatal(e);
                onError(e);
                return;
            }
            
            if (p == null) {
                s.cancel();

                onError(new NullPointerException("The mapper returned a null Publisher"));
                return;
            }
            
            if (p instanceof Callable) {
                R v;
                try {
                    v = ((Callable<R>)p).call();
                } catch (Throwable e) {
                    s.cancel();
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
                emitScalar(v);
            } else {
                PublisherFlatMapInner<R> inner = new PublisherFlatMapInner<>(this, prefetch);
                if (add(inner)) {
                    
                    p.subscribe(inner);
                }
            }
            
        }
        
        void emitScalar(R v) {
            if (v == null) {
                if (maxConcurrency != Integer.MAX_VALUE) {
                    int p = produced + 1;
                    if (p == limit) {
                        produced = 0;
                        s.request(p);
                    } else {
                        produced = p;
                    }
                }
                return;
            }
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
                } else {
                    Queue<R> q;
                    
                    try {
                        q = getOrCreateScalarQueue();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        
                        s.cancel();
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                            done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(ex);
                        }
                        
                        drainLoop();
                        return;
                    }
                    
                    if (!q.offer(v)) {
                        s.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                        drainLoop();
                        return;
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }

                drainLoop();
            } else {
                Queue<R> q;
                
                try {
                    q = getOrCreateScalarQueue();
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    
                    s.cancel();
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                        done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                    
                    drain();
                    return;
                }
                
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
                q = mainQueueSupplier.get();
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
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            drainLoop();
        }
        
        void drainLoop() {
            int missed = 1;
            
            final Subscriber<? super R> a = actual;
            
            for (;;) {
                
                boolean d = done;

                PublisherFlatMapInner<R>[] as = get();
                
                int n = as.length;

                Queue<R> sq = scalarQueue;
                
                boolean noSources = isEmpty();
                
                if (checkTerminated(d, noSources && (sq == null || sq.isEmpty()), a)) {
                    return;
                }
                
                boolean again = false;

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
                            e = 0L;
                            again = true;
                        }
                        
                    }
                }
                if (r != 0L && !noSources) {
                    
                    int j = lastIndex;
                    if (j >= n) {
                        j = 0;
                    }
                    
                    for (int i = 0; i < n; i++) {
                        if (cancelled) {
                            scalarQueue = null;
                            s.cancel();
                            unsubscribe();
                            return;
                        }
                        
                        PublisherFlatMapInner<R> inner = as[j];
                        if (inner != null) {
                            d = inner.done;
                            Queue<R> q = inner.queue;
                            if (d && q == null) {
                                remove(inner.index);
                                again = true;
                                replenishMain++;
                            } else 
                            if (q != null) {
                                while (e != r) {
                                    d = inner.done;
                                    
                                    R v;
                                    
                                    try {
                                        v = q.poll();
                                    } catch (Throwable ex) {
                                        ExceptionHelper.throwIfFatal(ex);
                                        inner.cancel();
                                        if (!ExceptionHelper.addThrowable(ERROR, this, ex)) {
                                            UnsignalledExceptions.onErrorDropped(ex);
                                        }
                                        v = null;
                                        d = true;
                                    }
                                    
                                    boolean empty = v == null;
                                    
                                    if (checkTerminated(d, false, a)) {
                                        return;
                                    }
    
                                    if (d && empty) {
                                        remove(inner.index);
                                        again = true;
                                        replenishMain++;
                                        break;
                                    }
                                    
                                    if (empty) {
                                        break;
                                    }
                                    
                                    a.onNext(v);
                                    
                                    e++;
                                }
                                
                                if (e == r) {
                                    d = inner.done;
                                    boolean empty;
                                    
                                    try {
                                        empty = q.isEmpty();
                                    } catch (Throwable ex) {
                                        ExceptionHelper.throwIfFatal(ex);
                                        inner.cancel();
                                        if (!ExceptionHelper.addThrowable(ERROR, this, ex)) {
                                            UnsignalledExceptions.onErrorDropped(ex);
                                        }
                                        empty = true;
                                        d = true;
                                    }
                                    
                                    if (d && empty) {
                                        remove(inner.index);
                                        again = true;
                                        replenishMain++;
                                    }
                                }
                                
                                if (e != 0L) {
                                    if (!inner.done) {
                                        inner.request(e);
                                    }
                                    if (r != Long.MAX_VALUE) {
                                        r = REQUESTED.addAndGet(this, -e);
                                        if (r == 0L) {
                                            break; // 0 .. n - 1
                                        }
                                    }
                                    e = 0L;
                                }
                            }
                        }
                        
                        if (r == 0L) {
                            break;
                        }
                        
                        if (++j == n) {
                            j = 0;
                        }
                    }
                    
                    lastIndex = j;
                }
                
                if (r == 0L && !noSources) {
                    as = get();
                    n = as.length;
                    
                    for (int i = 0; i < n; i++) {
                        if (cancelled) {
                            scalarQueue = null;
                            s.cancel();
                            unsubscribe();
                            return;
                        }
                        
                        PublisherFlatMapInner<R> inner = as[i];
                        if (inner == null) {
                            continue;
                        }
                        
                        d = inner.done;
                        Queue<R> q = inner.queue;
                        boolean empty = (q == null || q.isEmpty());
                        
                        // if we have a non-empty source then quit the cleanup
                        if (!empty) {
                            break;
                        }

                        if (d && empty) {
                            remove(inner.index);
                            again = true;
                            replenishMain++;
                        }
                    }
                }
                
                if (replenishMain != 0L && !done && !cancelled) {
                    s.request(replenishMain);
                }
                
                if (again) {
                    continue;
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
                unsubscribe();
                
                return true;
            }
            
            if (delayError) {
                if (d && empty) {
                    Throwable e = error;
                    if (e != null && e != ExceptionHelper.TERMINATED) {
                        e = ExceptionHelper.terminate(ERROR, this);
                        a.onError(e);
                    } else {
                        a.onComplete();
                    }
                    
                    return true;
                }
            } else {
                if (d) {
                    Throwable e = error;
                    if (e != null && e != ExceptionHelper.TERMINATED) {
                        e = ExceptionHelper.terminate(ERROR, this);
                        scalarQueue = null;
                        s.cancel();
                        unsubscribe();
                        
                        a.onError(e);
                        return true;
                    } else 
                    if (empty) {
                        a.onComplete();
                        return true;
                    }
                }
            }
            
            return false;
        }
        
        void innerError(PublisherFlatMapInner<R> inner, Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                inner.done = true;
                if (!delayError) {
                    done = true;
                }
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
                } else {
                    Queue<R> q;
                    
                    try {
                        q = getOrCreateScalarQueue(inner);
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        inner.cancel();
                        if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                            inner.done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(ex);
                        }
                        drainLoop();
                        return;
                    }
                    
                    if (!q.offer(v)) {
                        inner.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            inner.done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                        drainLoop();
                        return;
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }
                
                drainLoop();
            } else {
                Queue<R> q;
                
                try {
                    q = getOrCreateScalarQueue(inner);
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    inner.cancel();
                    if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                        inner.done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                    drain();
                    return;
                }
                
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
                q = innerQueueSupplier.get();
                inner.queue = q;
            }
            return q;
        }

        @Override
        public long getCapacity() {
            return maxConcurrency;
        }

        @Override
        public long getPending() {
            return done || scalarQueue == null ? -1L : scalarQueue.size();
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isStarted() {
            return s != null && !isTerminated() && !isCancelled();
        }

        @Override
        public boolean isTerminated() {
            return done && get().length == 0;
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Iterator<?> upstreams() {
            return Arrays.asList(get()).iterator();
        }

        @Override
        public long upstreamCount() {
            return get().length;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }
    
    static final class PublisherFlatMapInner<R> 
    implements Subscriber<R>, Subscription, Producer, Receiver, SubscriberState  {

        final PublisherFlatMapMain<?, R> parent;
        
        final int prefetch;
        
        final int limit;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapInner.class, Subscription.class, "s");
        
        long produced;
        
        volatile Queue<R> queue;
        
        volatile boolean done;
        
        /** Represents the optimization mode of this inner subscriber. */
        int sourceMode;
        
        /** Running with regular, arbitrary source. */
        static final int NORMAL = 0;
        /** Running with a source that implements SynchronousSource. */
        static final int SYNC = 1;
        /** Running with a source that implements AsynchronousSource. */
        static final int ASYNC = 2;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherFlatMapInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherFlatMapInner.class, "once");

        int index;
        
        public PublisherFlatMapInner(PublisherFlatMapMain<?, R> parent, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked") Fuseable.QueueSubscription<R> f = (Fuseable.QueueSubscription<R>)s;
                    int m = f.requestFusion(Fuseable.ANY);
                    if (m == Fuseable.SYNC){
                        sourceMode = SYNC;
                        queue = f;
                        done = true;
                        parent.drain();
                        return;
                    } else 
                    if (m == Fuseable.ASYNC) {
                        sourceMode = ASYNC;
                        queue = f;
                    }
                    // NONE is just fall-through as the queue will be created on demand
                }
                s.request(prefetch);
            }
        }

        @Override
        public void onNext(R t) {
            if (sourceMode == ASYNC) {
                parent.drain();
            } else {
                parent.innerNext(this, t);
            }
        }

        @Override
        public void onError(Throwable t) {
            // we don't want to emit the same error twice in case of subscription-race in async mode
            if (sourceMode != ASYNC || ONCE.compareAndSet(this, 0, 1)) {
                parent.innerError(this, t);
            }
        }

        @Override
        public void onComplete() {
            // onComplete is practically idempotent so there is no risk due to subscription-race in async mode
            done = true;
            parent.drain();
        }

        @Override
        public void request(long n) {
            if (sourceMode != SYNC) {
                long p = produced + n;
                if (p >= limit) {
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

        @Override
        public long getCapacity() {
            return prefetch;
        }

        @Override
        public long getPending() {
            return done || queue == null ? -1L : queue.size();
        }

        @Override
        public boolean isCancelled() {
            return s == SubscriptionHelper.cancelled();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done && !isCancelled();
        }

        @Override
        public boolean isTerminated() {
            return done && (queue == null || queue.isEmpty());
        }

        @Override
        public long expectedFromUpstream() {
            return produced;
        }

        @Override
        public long limit() {
            return limit;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object downstream() {
            return parent;
        }
    }
}
