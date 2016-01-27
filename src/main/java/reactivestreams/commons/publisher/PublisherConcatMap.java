package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import reactivestreams.commons.subscriber.SubscriberMultiSubscription;
import reactivestreams.commons.util.*;

/**
 * Maps each upstream value into a Publisher and concatenates them into one
 * sequence of items.
 * 
 * @param <T> the source value type
 * @param <R> the output value type
 */
public final class PublisherConcatMap<T, R> extends PublisherSource<T, R> {
    
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final Supplier<? extends Queue<T>> queueSupplier;
    
    final int prefetch;
    
    final ErrorMode errorMode;
    
    /**
     * Indicates when an error from the main source should be reported.
     */
    public enum ErrorMode {
        /** Report the error immediately, cancelling the active inner source. */
        IMMEDIATE,
        /** Report error after an inner source terminated. */
        BOUNDARY,
        /** Report the error after all sources terminated. */
        END
    }

    public PublisherConcatMap(Publisher<? extends T> source,
            Function<? super T, ? extends Publisher<? extends R>> mapper, 
            Supplier<? extends Queue<T>> queueSupplier,
            int prefetch, ErrorMode errorMode) {
        super(source);
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.prefetch = prefetch;
        this.errorMode = Objects.requireNonNull(errorMode, "errorMode");
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        
        if (PublisherFlatMap.scalarSubscribe(source, s, mapper)) {
            return;
        }
        
        Subscriber<T> parent = null;
        switch (errorMode) {
        case BOUNDARY:
            parent = new PublisherConcatMapDelayed<>(s, mapper, queueSupplier, prefetch, false);
            break;
        case END:
            parent = new PublisherConcatMapDelayed<>(s, mapper, queueSupplier, prefetch, true);
            break;
        default:
            parent = new PublisherConcatMapImmediate<>(s, mapper, queueSupplier, prefetch);
        }
        source.subscribe(parent);
    }
    
    static final class PublisherConcatMapImmediate<T, R> implements Subscriber<T>, PublisherConcatMapSupport<R>, Subscription {

        final Subscriber<? super R> actual;
        
        final PublisherConcatMapInner<R> inner;
        
        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final Supplier<? extends Queue<T>> queueSupplier;
        
        final int prefetch;

        final int limit;
        
        Subscription s;

        int consumed;
        
        volatile Queue<T> queue;
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherConcatMapImmediate, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherConcatMapImmediate.class, Throwable.class, "error");
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatMapImmediate> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherConcatMapImmediate.class, "wip");

        volatile int guard;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatMapImmediate> GUARD =
                AtomicIntegerFieldUpdater.newUpdater(PublisherConcatMapImmediate.class, "guard");

        int sourceMode;
        
        static final int SYNC = 1;
        static final int ASYNC = 2;
        
        public PublisherConcatMapImmediate(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper,
                Supplier<? extends Queue<T>> queueSupplier, int prefetch) {
            this.actual = actual;
            this.mapper = mapper;
            this.queueSupplier = queueSupplier;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.inner = new PublisherConcatMapInner<>(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s))  {
                this.s = s;

                if (s instanceof FusionSubscription) {
                    @SuppressWarnings("unchecked")
                    FusionSubscription<T> f = (FusionSubscription<T>)s;
                    queue = f;
                    if (f.requestSyncFusion()){
                        sourceMode = SYNC;
                        done = true;
                        
                        actual.onSubscribe(this);
                        
                        drain();
                        return;
                    } else {
                        sourceMode = ASYNC;
                    }
                } else {
                    try {
                        queue = queueSupplier.get();
                    } catch (Throwable ex) {
                        s.cancel();
                        
                        EmptySubscription.error(actual, ex);
                        return;
                    }
                }
                
                actual.onSubscribe(this);
                
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(T t) {
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
        public void innerNext(R value) {
            if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
                actual.onNext(value);
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
                        
                        T v;
                        
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
                                p = mapper.apply(v);
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


                            if (p instanceof Supplier) {
                                @SuppressWarnings("unchecked")
                                Supplier<R> supplier = (Supplier<R>) p;
                                
                                R vr = supplier.get();
                                if (vr == null) {
                                    continue;
                                }
                                
                                if (inner.isUnbounded()) {
                                    if (guard == 0 && GUARD.compareAndSet(this, 0, 1)) {
                                        actual.onNext(vr);
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
                                    active = true;
                                    inner.set(new WeakScalarSubscription<>(vr, inner));
                                }
                                
                            } else {
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

    static final class PublisherConcatMapDelayed<T, R> implements Subscriber<T>, PublisherConcatMapSupport<R>, Subscription {

        final Subscriber<? super R> actual;
        
        final PublisherConcatMapInner<R> inner;
        
        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final Supplier<? extends Queue<T>> queueSupplier;
        
        final int prefetch;

        final int limit;
        
        final boolean veryEnd;
        
        Subscription s;

        int consumed;
        
        volatile Queue<T> queue;
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherConcatMapDelayed, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherConcatMapDelayed.class, Throwable.class, "error");
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatMapDelayed> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherConcatMapDelayed.class, "wip");

        int sourceMode;
        
        static final int SYNC = 1;
        static final int ASYNC = 2;
        
        public PublisherConcatMapDelayed(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper,
                Supplier<? extends Queue<T>> queueSupplier, int prefetch, boolean veryEnd) {
            this.actual = actual;
            this.mapper = mapper;
            this.queueSupplier = queueSupplier;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.veryEnd = veryEnd;
            this.inner = new PublisherConcatMapInner<>(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s))  {
                this.s = s;

                if (s instanceof FusionSubscription) {
                    @SuppressWarnings("unchecked")
                    FusionSubscription<T> f = (FusionSubscription<T>)s;
                    queue = f;
                    if (f.requestSyncFusion()){
                        sourceMode = SYNC;
                        done = true;
                        
                        actual.onSubscribe(this);
                        
                        drain();
                        return;
                    } else {
                        sourceMode = ASYNC;
                    }
                } else {
                    try {
                        queue = queueSupplier.get();
                    } catch (Throwable ex) {
                        s.cancel();
                        
                        EmptySubscription.error(actual, ex);
                        return;
                    }
                }
                
                actual.onSubscribe(this);
                
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(T t) {
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
        public void innerNext(R value) {
            actual.onNext(value);
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
                        
                        T v;
                        
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
                                p = mapper.apply(v);
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
                            
                            if (p instanceof Supplier) {
                                @SuppressWarnings("unchecked")
                                Supplier<R> supplier = (Supplier<R>) p;
                                
                                R vr = supplier.get();
                                if (vr == null) {
                                    continue;
                                }
                                
                                if (inner.isUnbounded()) {
                                    actual.onNext(vr);
                                    continue;
                                } else {
                                    active = true;
                                    inner.set(new WeakScalarSubscription<>(vr, inner));
                                }
                            } else {
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

    interface PublisherConcatMapSupport<T> {
        
        void innerNext(T value);
        
        void innerComplete();
        
        void innerError(Throwable e);
    }
    
    static final class PublisherConcatMapInner<R>
    extends SubscriberMultiSubscription<R, R> {
        
        final PublisherConcatMapSupport<R> parent;
        
        long produced;
        
        public PublisherConcatMapInner(PublisherConcatMapSupport<R> parent) {
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
            
            parent.innerNext(t);
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
