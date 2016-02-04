/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.MultiReceiver;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.state.Backpressurable;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.state.Failurable;
import reactivestreams.commons.state.Introspectable;
import reactivestreams.commons.state.Prefetchable;
import reactivestreams.commons.state.Requestable;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.CancelledSubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.ScalarSubscription;
import reactivestreams.commons.util.SpscArrayQueue;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

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
    public void subscribe(Subscriber<? super R> s) {
        
        if (ScalarSubscription.trySubscribeScalarMap(source, s, mapper)) {
            return;
        }
        
//        source.subscribe(new PublisherFlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier));
        source.subscribe(new MergeSubscriber<>(s, mapper, delayError, maxConcurrency, prefetch));
    }

    static final class PublisherFlatMapMain<T, R> 
    implements Subscriber<T>, Subscription, Receiver, MultiReceiver, Requestable, Completable, Producer,
               Cancellable, Backpressurable, Failurable {
        
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
        
        volatile PublisherFlatMapInner<R>[] subscribers;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapMain, PublisherFlatMapInner[]> SUBSCRIBERS =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapMain.class, PublisherFlatMapInner[].class, "subscribers");
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] EMPTY = new PublisherFlatMapInner[0];
        
        @SuppressWarnings("rawtypes")
        static final PublisherFlatMapInner[] TERMINATED = new PublisherFlatMapInner[0];
        
        int last;
        
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
                    System.arraycopy(a, 0, b, 0, j);
                    System.arraycopy(a, j + 1, b, j, n - j - 1);
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
            
            if (p instanceof Supplier) {
                R v;
                try {
                    v = ((Supplier<R>)p).get();
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
                    
                    if (WIP.decrementAndGet(this) == 0) {
                        return;
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
                    }
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
                
                PublisherFlatMapInner<R>[] as = subscribers;
                
                int n = as.length;

                boolean d = done;
                Queue<R> sq = scalarQueue;
                
                if (checkTerminated(d, n == 0 && (sq == null || sq.isEmpty()), a)) {
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

                    if (r != 0L) {
                        
                        int j = last;
                        if (j >= n) {
                            j = n - 1;
                        }
                        
                        for (int i = 0; i < n; i++) {
                            if (cancelled) {
                                return;
                            }
                            
                            PublisherFlatMapInner<R> inner = as[j];
                            
                            d = inner.done;
                            Queue<R> q = inner.queue;
                            if (d && q == null) {
                                remove(inner);
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
                                        remove(inner);
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
                                        remove(inner);
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
                                            last = j;
                                            break; // 0 .. n - 1
                                        }
                                    }
                                    e = 0L;
                                }
                            }
                            if (e == r) {
                                last = i;
                                break;
                            }
                            if (++j == n) {
                                j = 0;
                            }
                        }
                    }
                }
                
                if (r == 0L) {
                    as = subscribers;
                    n = as.length;
                    
                    for (int i = 0; i < n; i++) {
                        if (cancelled) {
                            return;
                        }
                        
                        PublisherFlatMapInner<R> inner = as[i];
                        
                        d = inner.done;
                        Queue<R> q = inner.queue;
                        if (d && (q == null || q.isEmpty())) {
                            remove(inner);
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
                cancelAllInner();
                
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
                        cancelAllInner();
                        
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
                    
                    if (WIP.decrementAndGet(this) == 0) {
                        return;
                    }
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
                    }
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
            return done && subscribers.length == 0;
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
            return Arrays.asList(subscribers).iterator();
        }

        @Override
        public long upstreamCount() {
            return subscribers.length;
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
    implements Subscriber<R>, Subscription, Producer, Receiver,
               Backpressurable,
               Cancellable,
               Completable,
               Prefetchable,
               Introspectable {

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
            return s == CancelledSubscription.INSTANCE;
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
        public int getMode() {
            return INNER;
        }

        @Override
        public String getName() {
            return PublisherFlatMapInner.class.getSimpleName();
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
    
    static final class MergeSubscriber<T, U> extends AtomicInteger implements Subscription, Subscriber<T> {
        /** */
        private static final long serialVersionUID = -2117620485640801370L;
        
        final Subscriber<? super U> actual;
        final Function<? super T, ? extends Publisher<? extends U>> mapper;
        final boolean delayErrors;
        final int maxConcurrency;
        final int bufferSize;
        
        volatile Queue<U> queue;
        
        volatile boolean done;
        
        volatile Queue<Throwable> errors;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MergeSubscriber, Queue> ERRORS =
                AtomicReferenceFieldUpdater.newUpdater(MergeSubscriber.class, Queue.class, "errors");
        
        static final Queue<Throwable> ERRORS_CLOSED = new RejectingQueue<>();
        
        volatile boolean cancelled;
        
        volatile InnerSubscriber<?, ?>[] subscribers;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MergeSubscriber, InnerSubscriber[]> SUBSCRIBERS =
                AtomicReferenceFieldUpdater.newUpdater(MergeSubscriber.class, InnerSubscriber[].class, "subscribers");
        
        static final InnerSubscriber<?, ?>[] EMPTY = new InnerSubscriber<?, ?>[0];
        
        static final InnerSubscriber<?, ?>[] CANCELLED = new InnerSubscriber<?, ?>[0];
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<MergeSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(MergeSubscriber.class, "requested");
        
        Subscription s;
        
        long uniqueId;
        long lastId;
        int lastIndex;
        
        public MergeSubscriber(Subscriber<? super U> actual, Function<? super T, ? extends Publisher<? extends U>> mapper,
                boolean delayErrors, int maxConcurrency, int bufferSize) {
            this.actual = actual;
            this.mapper = mapper;
            this.delayErrors = delayErrors;
            this.maxConcurrency = maxConcurrency;
            this.bufferSize = bufferSize;
            SUBSCRIBERS.lazySet(this, EMPTY);
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (!SubscriptionHelper.validate(this.s, s)) {
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
            if (!cancelled) {
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
            // safeguard against misbehaving sources
            if (done) {
                return;
            }
            Publisher<? extends U> p;
            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                onError(e);
                return;
            }
            if (p instanceof Supplier) {
                tryEmitScalar(((Supplier<? extends U>)p).get());
            } else {
                InnerSubscriber<T, U> inner = new InnerSubscriber<>(this, uniqueId++);
                addInner(inner);
                p.subscribe(inner);
            }
        }
        
        void addInner(InnerSubscriber<T, U> inner) {
            for (;;) {
                InnerSubscriber<?, ?>[] a = subscribers;
                if (a == CANCELLED) {
                    inner.dispose();
                    return;
                }
                int n = a.length;
                InnerSubscriber<?, ?>[] b = new InnerSubscriber[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return;
                }
            }
        }
        
        void removeInner(InnerSubscriber<T, U> inner) {
            for (;;) {
                InnerSubscriber<?, ?>[] a = subscribers;
                if (a == CANCELLED || a == EMPTY) {
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
                InnerSubscriber<?, ?>[] b;
                if (n == 1) {
                    b = EMPTY;
                } else {
                    b = new InnerSubscriber<?, ?>[n - 1];
                    System.arraycopy(a, 0, b, 0, j);
                    System.arraycopy(a, j + 1, b, j, n - j - 1);
                }
                if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                    return;
                }
            }
        }
        
        Queue<U> getMainQueue() {
            Queue<U> q = queue;
            if (q == null) {
                if (maxConcurrency == Integer.MAX_VALUE) {
                    q = new ConcurrentLinkedQueue<>();
                } else {
//                    if (Pow2.isPowerOfTwo(maxConcurrency)) {
                        q = new SpscArrayQueue<>(maxConcurrency);
//                    } else {
//                        q = new SpscExactArrayQueue<>(maxConcurrency);
//                    }
                }
                queue = q;
            }
            return q;
        }
        
        void tryEmitScalar(U value) {
            if (value == null) {
                if (maxConcurrency != Integer.MAX_VALUE && !cancelled) {
                    s.request(1);
                }
                return;
            }
            if (get() == 0 && compareAndSet(0, 1)) {
                long r = requested;
                if (r != 0L) {
                    actual.onNext(value);
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    if (maxConcurrency != Integer.MAX_VALUE && !cancelled) {
                        s.request(1);
                    }
                } else {
                    Queue<U> q = getMainQueue();
                    if (!q.offer(value)) {
                        onError(new IllegalStateException("Scalar queue full?!"));
                        return;
                    }
                }
                if (decrementAndGet() == 0) {
                    return;
                }
            } else {
                Queue<U> q = getMainQueue();
                if (!q.offer(value)) {
                    onError(new IllegalStateException("Scalar queue full?!"));
                    return;
                }
                if (getAndIncrement() != 0) {
                    return;
                }
            }
            drainLoop();
        }
        
        Queue<U> getInnerQueue(InnerSubscriber<T, U> inner) {
            Queue<U> q = inner.queue;
            if (q == null) {
                q = new SpscArrayQueue<>(bufferSize);
                inner.queue = q;
            }
            return q;
        }
        
        void tryEmit(U value, InnerSubscriber<T, U> inner) {
            if (get() == 0 && compareAndSet(0, 1)) {
                long r = requested;
                if (r != 0L) {
                    actual.onNext(value);
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    inner.requestMore(1);
                } else {
                    Queue<U> q = getInnerQueue(inner);
                    if (!q.offer(value)) {
                        onError(new IllegalStateException("Inner queue full?!"));
                        return;
                    }
                }
                if (decrementAndGet() == 0) {
                    return;
                }
            } else {
                Queue<U> q = inner.queue;
                if (q == null) {
                    q = new SpscArrayQueue<>(bufferSize);
                    inner.queue = q;
                }
                if (!q.offer(value)) {
                    onError(new IllegalStateException("Inner queue full?!"));
                    return;
                }
                if (getAndIncrement() != 0) {
                    return;
                }
            }
            drainLoop();
        }
        
        @Override
        public void onError(Throwable t) {
            // safeguard against misbehaving sources
            if (done) {
                return;
            }
            getErrorQueue().offer(t);
            done = true;
            drain();
        }
        
        @Override
        public void onComplete() {
            // safeguard against misbehaving sources
            if (done) {
                return;
            }
            done = true;
            drain();
        }
        
        @Override
        public void request(long n) {
            if (!SubscriptionHelper.validate(n)) {
                return;
            }
            BackpressureHelper.addAndGet(REQUESTED, this, n);
            drain();
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                if (getAndIncrement() == 0) {
                    s.cancel();
                    unsubscribe();
                }
            }
        }
        
        void drain() {
            if (getAndIncrement() == 0) {
                drainLoop();
            }
        }
        
        void drainLoop() {
            final Subscriber<? super U> child = this.actual;
            int missed = 1;
            for (;;) {
                if (checkTerminate()) {
                    return;
                }
                Queue<U> svq = queue;
                
                long r = requested;
                boolean unbounded = r == Long.MAX_VALUE;
                
                long replenishMain = 0;

                if (svq != null) {
                    for (;;) {
                        long scalarEmission = 0;
                        U o = null;
                        while (r != 0L) {
                            o = svq.poll();
                            if (checkTerminate()) {
                                return;
                            }
                            if (o == null) {
                                break;
                            }
                            
                            child.onNext(o);
                            
                            replenishMain++;
                            scalarEmission++;
                            r--;
                        }
                        if (scalarEmission != 0L) {
                            if (unbounded) {
                                r = Long.MAX_VALUE;
                            } else {
                                r = REQUESTED.addAndGet(this, -scalarEmission);
                            }
                        }
                        if (r == 0L || o == null) {
                            break;
                        }
                    }
                }

                boolean d = done;
                svq = queue;
                InnerSubscriber<?, ?>[] inner = subscribers;
                int n = inner.length;
                
                if (d && (svq == null || svq.isEmpty()) && n == 0) {
                    Queue<Throwable> e = errors;
                    if (e == null || e.isEmpty()) {
                        child.onComplete();
                    } else {
                        reportError(e);
                    }
                    return;
                }
                
                boolean innerCompleted = false;
                if (n != 0) {
                    long startId = lastId;
                    int index = lastIndex;
                    
                    if (n <= index || inner[index].id != startId) {
                        if (n <= index) {
                            index = 0;
                        }
                        int j = index;
                        for (int i = 0; i < n; i++) {
                            if (inner[j].id == startId) {
                                break;
                            }
                            j++;
                            if (j == n) {
                                j = 0;
                            }
                        }
                        index = j;
                        lastIndex = j;
                        lastId = inner[j].id;
                    }
                    
                    int j = index;
                    for (int i = 0; i < n; i++) {
                        if (checkTerminate()) {
                            return;
                        }
                        @SuppressWarnings("unchecked")
                        InnerSubscriber<T, U> is = (InnerSubscriber<T, U>)inner[j];
                        
                        U o = null;
                        for (;;) {
                            long produced = 0;
                            while (r != 0L) {
                                if (checkTerminate()) {
                                    return;
                                }
                                Queue<U> q = is.queue;
                                if (q == null) {
                                    break;
                                }
                                o = q.poll();
                                if (o == null) {
                                    break;
                                }

                                child.onNext(o);
                                
                                r--;
                                produced++;
                            }
                            if (produced != 0L) {
                                if (!unbounded) {
                                    r = REQUESTED.addAndGet(this, -produced);
                                } else {
                                    r = Long.MAX_VALUE;
                                }
                                is.requestMore(produced);
                            }
                            if (r == 0 || o == null) {
                                break;
                            }
                        }
                        boolean innerDone = is.done;
                        Queue<U> innerQueue = is.queue;
                        if (innerDone && (innerQueue == null || innerQueue.isEmpty())) {
                            removeInner(is);
                            if (checkTerminate()) {
                                return;
                            }
                            replenishMain++;
                            innerCompleted = true;
                        }
                        if (r == 0L) {
                            break;
                        }
                        
                        j++;
                        if (j == n) {
                            j = 0;
                        }
                    }
                    lastIndex = j;
                    lastId = inner[j].id;
                }
                
                if (replenishMain != 0L && !cancelled) {
                    s.request(replenishMain);
                }
                if (innerCompleted) {
                    continue;
                }
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminate() {
            if (cancelled) {
                s.cancel();
                unsubscribe();
                return true;
            }
            Queue<Throwable> e = errors;
            if (!delayErrors && (e != null && !e.isEmpty())) {
                try {
                    reportError(e);
                } finally {
                    unsubscribe();
                }
                return true;
            }
            return false;
        }
        
        void reportError(Queue<Throwable> q) {
            Throwable ex = null;
            
            Throwable t;
            int count = 0;
            while ((t = q.poll()) != null) {
                if (count == 0) {
                    ex = t;
                } else {
                    ex.addSuppressed(t);
                }
                
                count++;
            }
            actual.onError(ex);
        }
        
        void unsubscribe() {
            InnerSubscriber<?, ?>[] a = subscribers;
            if (a != CANCELLED) {
                a = SUBSCRIBERS.getAndSet(this, CANCELLED);
                if (a != CANCELLED) {
                    ERRORS.getAndSet(this, ERRORS_CLOSED);
                    for (InnerSubscriber<?, ?> inner : a) {
                        inner.dispose();
                    }
                }
            }
        }
        
        Queue<Throwable> getErrorQueue() {
            for (;;) {
                Queue<Throwable> q = errors;
                if (q != null) {
                    return q;
                }
                q = new ConcurrentLinkedQueue<>();
                if (ERRORS.compareAndSet(this, null, q)) {
                    return q;
                }
            }
        }
    }
    
    static final class InnerSubscriber<T, U> extends AtomicReference<Subscription> 
    implements Subscriber<U> {
        /** */
        private static final long serialVersionUID = -4606175640614850599L;
        final long id;
        final MergeSubscriber<T, U> parent;
        final int limit;
        final int bufferSize;
        
        volatile boolean done;
        volatile Queue<U> queue;
        int outstanding;
        
        static final Subscription CANCELLED = new Subscription() {
            @Override
            public void request(long n) {
                
            }
            
            @Override
            public void cancel() {
                
            }
        };
        
        public InnerSubscriber(MergeSubscriber<T, U> parent, long id) {
            this.id = id;
            this.parent = parent;
            this.bufferSize = parent.bufferSize;
            this.limit = bufferSize >> 2;
        }
        @Override
        public void onSubscribe(Subscription s) {
            if (!compareAndSet(null, s)) {
                s.cancel();
                if (get() != CANCELLED) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
                return;
            }
            outstanding = bufferSize;
            s.request(outstanding);
        }
        @Override
        public void onNext(U t) {
            parent.tryEmit(t, this);
        }
        @Override
        public void onError(Throwable t) {
            parent.getErrorQueue().offer(t);
            done = true;
            parent.drain();
        }
        @Override
        public void onComplete() {
            done = true;
            parent.drain();
        }
        
        void requestMore(long n) {
            int r = outstanding - (int)n;
            if (r > limit) {
                outstanding = r;
                return;
            }
            outstanding = bufferSize;
            int k = bufferSize - r;
            if (k > 0) {
                get().request(k);
            }
        }
        
        void dispose() {
            Subscription s = get();
            if (s != CANCELLED) {
                s = getAndSet(CANCELLED);
                if (s != CANCELLED && s != null) {
                    s.cancel();
                }
            }
        }
    }
    
    static final class RejectingQueue<T> extends AbstractQueue<T> {
        @Override
        public boolean offer(T e) {
            return false;
        }

        @Override
        public T poll() {
            return null;
        }

        @Override
        public T peek() {
            return null;
        }

        @Override
        public Iterator<T> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public int size() {
            return 0;
        }
        
    }
}
