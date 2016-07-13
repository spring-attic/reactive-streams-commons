package rsc.parallel;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.flow.Fuseable;
import rsc.publisher.PublisherConcatMap.ErrorMode;

import rsc.subscriber.MultiSubscriptionSubscriber;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Concatenates the generated Publishers on each rail.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public final class ParallelOrderedConcatMap<T, R> extends ParallelOrderedBase<R> {

    final ParallelOrderedBase<T> source;
    
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final Supplier<? extends Queue<OrderedItem<T>>> queueSupplier;
    
    final int prefetch;
    
    final ErrorMode errorMode;

    public ParallelOrderedConcatMap(
            ParallelOrderedBase<T> source, 
            Function<? super T, ? extends Publisher<? extends R>> mapper, 
                    Supplier<? extends Queue<OrderedItem<T>>> queueSupplier,
                    int prefetch, ErrorMode errorMode) {
        this.source = source;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.prefetch = prefetch;
        this.errorMode = Objects.requireNonNull(errorMode, "errorMode");
    }
    
    @Override
    public int parallelism() {
        return source.parallelism();
    }
    
    @Override
    public void subscribeOrdered(Subscriber<? super OrderedItem<R>>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        
        @SuppressWarnings("unchecked")
        Subscriber<OrderedItem<T>>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            Subscriber<? super OrderedItem<R>> s = subscribers[i];
            Subscriber<OrderedItem<T>> parent = null;
            switch (errorMode) {
            case BOUNDARY:
                parent = new ParallelConcatMapDelayed<>(s, mapper, queueSupplier, prefetch, false);
                break;
            case END:
                parent = new ParallelConcatMapDelayed<>(s, mapper, queueSupplier, prefetch, true);
                break;
            default:
                parent = new ParallelConcatMapImmediate<>(s, mapper, queueSupplier, prefetch);
            }
            parents[i] = parent;
        }
        
        source.subscribeOrdered(parents);
    }
    
    static final class ParallelConcatMapImmediate<T, R> implements Subscriber<OrderedItem<T>>, ParallelConcatMapSupport<R>, Subscription {

        final Subscriber<? super OrderedItem<R>> actual;
        
        final ParallelConcatMapInner<R> inner;
        
        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final Supplier<? extends Queue<OrderedItem<T>>> queueSupplier;
        
        final int prefetch;

        final int limit;
        
        Subscription s;

        int consumed;
        
        volatile Queue<OrderedItem<T>> queue;
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ParallelConcatMapImmediate, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(ParallelConcatMapImmediate.class, Throwable.class, "error");
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ParallelConcatMapImmediate> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ParallelConcatMapImmediate.class, "wip");

        volatile int guard;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ParallelConcatMapImmediate> GUARD =
                AtomicIntegerFieldUpdater.newUpdater(ParallelConcatMapImmediate.class, "guard");

        int sourceMode;
        
        static final int SYNC = 1;
        static final int ASYNC = 2;
        
        public ParallelConcatMapImmediate(Subscriber<? super OrderedItem<R>> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper,
                Supplier<? extends Queue<OrderedItem<T>>> queueSupplier, int prefetch) {
            this.actual = actual;
            this.mapper = mapper;
            this.queueSupplier = queueSupplier;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.inner = new ParallelConcatMapInner<>(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s))  {
                this.s = s;

                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked") Fuseable.QueueSubscription<OrderedItem<T>> f = (Fuseable.QueueSubscription<OrderedItem<T>>)s;
                    int m = f.requestFusion(Fuseable.ANY);
                    if (m == Fuseable.SYNC){
                        sourceMode = SYNC;
                        queue = f;
                        done = true;
                        
                        actual.onSubscribe(this);
                        
                        drain();
                        return;
                    } else 
                    if (m == Fuseable.ASYNC) {
                        sourceMode = ASYNC;
                        queue = f;
                    } else {
                        try {
                            queue = queueSupplier.get();
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            s.cancel();
                            SubscriptionHelper.error(actual, ex);
                            return;
                        }
                    }
                } else {
                    try {
                        queue = queueSupplier.get();
                    } catch (Throwable ex) {
                        s.cancel();
                        
                        SubscriptionHelper.error(actual, ex);
                        return;
                    }
                }
                
                actual.onSubscribe(this);
                
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(OrderedItem<T> t) {
            if (sourceMode == ASYNC) {
                drain();
            } else
            if (!queue.offer(t)) {
                s.cancel();
                onError(new IllegalStateException("Queue full?!"));
            } else {
                drain();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                inner.cancel();
                
                if (GUARD.getAndIncrement(this) == 0) {
                    t = ExceptionHelper.terminate(ERROR, this);
                    if (t != ExceptionHelper.TERMINATED) {
                        actual.onError(t);
                    }
                }
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            done = true;
            drain();
        }
        
        @Override
        public void innerNext(R value, long parentIndex) {
            if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
                actual.onNext(PrimaryOrderedItem.of(value, parentIndex));
                if (GUARD.compareAndSet(this, 1, 0)) {
                    return;
                }
                Throwable e = ExceptionHelper.terminate(ERROR, this);
                if (e != ExceptionHelper.TERMINATED) {
                    actual.onError(e);
                }
            }
        }
        
        @Override
        public void innerComplete() {
            active = false;
            drain();
        }
        
        @Override
        public void innerError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                s.cancel();
                
                if (GUARD.getAndIncrement(this) == 0) {
                    e = ExceptionHelper.terminate(ERROR, this);
                    if (e != ExceptionHelper.TERMINATED) {
                        actual.onError(e);
                    }
                }
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        @Override
        public void request(long n) {
            inner.request(n);
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                inner.cancel();
                s.cancel();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) == 0) {
                for (;;) {
                    if (cancelled) {
                        return;
                    }
                    
                    if (!active) {
                        boolean d = done;
                        
                        OrderedItem<T> v;
                        
                        try {
                            v = queue.poll();
                        } catch (Throwable e) {
                            ExceptionHelper.throwIfFatal(e);
                            s.cancel();
                            actual.onError(e);
                            return;
                        }
                        
                        boolean empty = v == null;
                        
                        if (d && empty) {
                            actual.onComplete();
                            return;
                        }
                        
                        if (!empty) {
                            Publisher<? extends R> p;
                            
                            try {
                                p = mapper.apply(v.get());
                            } catch (Throwable e) {
                                ExceptionHelper.throwIfFatal(e);
                                
                                s.cancel();
                                actual.onError(e);
                                return;
                            }
                            
                            if (p == null) {
                                s.cancel();
                                actual.onError(new NullPointerException("The mapper returned a null Publisher"));
                                return;
                            }
                            
                            if (sourceMode != SYNC) {
                                int c = consumed + 1;
                                if (c == limit) {
                                    consumed = 0;
                                    s.request(c);
                                } else {
                                    consumed = c;
                                }
                            }


                            if (p instanceof Callable) {
                                @SuppressWarnings("unchecked")
                                Callable<R> callable = (Callable<R>) p;
                                
                                R vr;
                                
                                try {
                                    vr = callable.call();
                                } catch (Throwable e) {
                                    ExceptionHelper.throwIfFatal(e);
                                    s.cancel();
                                    actual.onError(ExceptionHelper.unwrap(e));
                                    return;
                                }
                                
                                
                                if (vr == null) {
                                    continue;
                                }
                                
                                if (inner.isUnbounded()) {
                                    if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
                                        actual.onNext(v.change(vr));
                                        if (!GUARD.compareAndSet(this, 1, 0)) {
                                            Throwable e = ExceptionHelper.terminate(ERROR, this);
                                            if (e != ExceptionHelper.TERMINATED) {
                                                actual.onError(e);
                                            }
                                            return;
                                        }
                                    }
                                    continue;
                                } else {
                                    inner.parentIndex = v.index();
                                    active = true;
                                    inner.set(new WeakScalarSubscription<>(vr, inner));
                                }
                                
                            } else {
                                inner.parentIndex = v.index();
                                active = true;
                                p.subscribe(inner);
                            }
                        }
                    }
                    if (WIP.decrementAndGet(this) == 0) {
                        break;
                    }
                }
            }
        }
    }
    
    static final class WeakScalarSubscription<T> implements Subscription {
        final Subscriber<? super T> actual;
        final T value;
        boolean once;

        public WeakScalarSubscription(T value, Subscriber<? super T> actual) {
            this.value = value;
            this.actual = actual;
        }
        
        @Override
        public void request(long n) {
            if (n > 0 && !once) {
                once = true;
                Subscriber<? super T> a = actual;
                a.onNext(value);
                a.onComplete();
            }
        }
        
        @Override
        public void cancel() {
            
        }
    }

    static final class ParallelConcatMapDelayed<T, R> implements Subscriber<OrderedItem<T>>, ParallelConcatMapSupport<R>, Subscription {

        final Subscriber<? super OrderedItem<R>> actual;
        
        final ParallelConcatMapInner<R> inner;
        
        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final Supplier<? extends Queue<OrderedItem<T>>> queueSupplier;
        
        final int prefetch;

        final int limit;
        
        final boolean veryEnd;
        
        Subscription s;

        int consumed;
        
        volatile Queue<OrderedItem<T>> queue;
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ParallelConcatMapDelayed, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(ParallelConcatMapDelayed.class, Throwable.class, "error");
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ParallelConcatMapDelayed> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ParallelConcatMapDelayed.class, "wip");

        int sourceMode;
        
        static final int SYNC = 1;
        static final int ASYNC = 2;
        
        public ParallelConcatMapDelayed(Subscriber<? super OrderedItem<R>> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper,
                Supplier<? extends Queue<OrderedItem<T>>> queueSupplier, int prefetch, boolean veryEnd) {
            this.actual = actual;
            this.mapper = mapper;
            this.queueSupplier = queueSupplier;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.veryEnd = veryEnd;
            this.inner = new ParallelConcatMapInner<>(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s))  {
                this.s = s;

                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked") Fuseable.QueueSubscription<OrderedItem<T>> f = (Fuseable.QueueSubscription<OrderedItem<T>>)s;
                    
                    int m = f.requestFusion(Fuseable.ANY);
                    
                    if (m == Fuseable.SYNC){
                        sourceMode = SYNC;
                        queue = f;
                        done = true;
                        
                        actual.onSubscribe(this);
                        
                        drain();
                        return;
                    } else 
                    if (m == Fuseable.ASYNC) {
                        sourceMode = ASYNC;
                        queue = f;
                    } else {
                        try {
                            queue = queueSupplier.get();
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            s.cancel();
                            SubscriptionHelper.error(actual, ex);
                            return;
                        }
                    }
                } else {
                    try {
                        queue = queueSupplier.get();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        s.cancel();
                        SubscriptionHelper.error(actual, ex);
                        return;
                    }
                }
                
                actual.onSubscribe(this);
                
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(OrderedItem<T> t) {
            if (sourceMode == ASYNC) {
                drain();
            } else
            if (!queue.offer(t)) {
                s.cancel();
                onError(new IllegalStateException("Queue full?!"));
            } else {
                drain();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            done = true;
            drain();
        }
        
        @Override
        public void innerNext(R value, long parentIndex) {
            actual.onNext(PrimaryOrderedItem.of(value, parentIndex));
        }
        
        @Override
        public void innerComplete() {
            active = false;
            drain();
        }
        
        @Override
        public void innerError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                if (!veryEnd) {
                    s.cancel();
                    done = true;
                }
                active = false;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        @Override
        public void request(long n) {
            inner.request(n);
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                inner.cancel();
                s.cancel();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) == 0) {
                
                for (;;) {
                    if (cancelled) {
                        return;
                    }
                    
                    if (!active) {
                        
                        boolean d = done;
                        
                        if (d && !veryEnd) {
                            Throwable ex = error;
                            if (ex != null) {
                                ex = ExceptionHelper.terminate(ERROR, this);
                                if (ex != ExceptionHelper.TERMINATED) {
                                    actual.onError(ex);
                                }
                                return;
                            }
                        }
                        
                        OrderedItem<T> v;
                        
                        try {
                            v = queue.poll();
                        } catch (Throwable e) {
                            ExceptionHelper.throwIfFatal(e);
                            s.cancel();
                            actual.onError(e);
                            return;
                        }
                        
                        boolean empty = v == null;
                        
                        if (d && empty) {
                            Throwable ex = ExceptionHelper.terminate(ERROR, this);
                            if (ex != null && ex != ExceptionHelper.TERMINATED) {
                                actual.onError(ex);
                            } else {
                                actual.onComplete();
                            }
                            return;
                        }
                        
                        if (!empty) {
                            Publisher<? extends R> p;
                            
                            try {
                                p = mapper.apply(v.get());
                            } catch (Throwable e) {
                                ExceptionHelper.throwIfFatal(e);
                                
                                s.cancel();
                                actual.onError(e);
                                return;
                            }
                            
                            if (p == null) {
                                s.cancel();
                                actual.onError(new NullPointerException("The mapper returned a null Publisher"));
                                return;
                            }
                            
                            if (sourceMode != SYNC) {
                                int c = consumed + 1;
                                if (c == limit) {
                                    consumed = 0;
                                    s.request(c);
                                } else {
                                    consumed = c;
                                }
                            }
                            
                            if (p instanceof Callable) {
                                @SuppressWarnings("unchecked")
                                Callable<R> supplier = (Callable<R>) p;
                                
                                R vr;
                                
                                try {
                                    vr = supplier.call();
                                } catch (Throwable e) {
                                    ExceptionHelper.throwIfFatal(e);
                                    s.cancel();
                                    actual.onError(ExceptionHelper.unwrap(e));
                                    return;
                                }
                                
                                if (vr == null) {
                                    continue;
                                }
                                
                                if (inner.isUnbounded()) {
                                    actual.onNext(v.change(vr));
                                    continue;
                                } else {
                                    inner.parentIndex = v.index();
                                    active = true;
                                    inner.set(new WeakScalarSubscription<>(vr, inner));
                                }
                            } else {
                                inner.parentIndex = v.index();
                                active = true;
                                p.subscribe(inner);
                            }
                        }
                    }
                    if (WIP.decrementAndGet(this) == 0) {
                        break;
                    }
                }
            }
        }
    }
    
    interface ParallelConcatMapSupport<T> {
        
        void innerNext(T value, long parentIndex);
        
        void innerComplete();
        
        void innerError(Throwable e);
    }
    
    static final class ParallelConcatMapInner<R>
            extends MultiSubscriptionSubscriber<R, R> {
        
        final ParallelConcatMapSupport<R> parent;
        
        long parentIndex;
        
        long produced;
        
        public ParallelConcatMapInner(ParallelConcatMapSupport<R> parent) {
            super(null);
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }
        
        @Override
        public void onNext(R t) {
            produced++;
            
            parent.innerNext(t, parentIndex);
        }
        
        @Override
        public void onError(Throwable t) {
            long p = produced;
            
            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            parent.innerError(t);
        }
        
        @Override
        public void onComplete() {
            long p = produced;
            
            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            parent.innerComplete();
        }
    }
}
