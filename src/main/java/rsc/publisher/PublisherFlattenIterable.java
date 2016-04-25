package rsc.publisher;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.flow.*;
import rsc.util.*;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, innerInput = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SCALAR, FusionMode.SYNC, FusionMode.ASYNC }, innerInput = { FusionMode.SYNC }, output = { FusionMode.SYNC })
public final class PublisherFlattenIterable<T, R> extends PublisherSource<T, R> implements Fuseable {

    final Function<? super T, ? extends Iterable<? extends R>> mapper;
    
    final int prefetch;
    
    final Supplier<Queue<T>> queueSupplier;
    
    public PublisherFlattenIterable(Publisher<? extends T> source,
            Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch,
            Supplier<Queue<T>> queueSupplier) {
        super(source);
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.prefetch = prefetch;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super R> s) {
        if (source instanceof Callable) {
            T v;
            
            try {
                v = ((Callable<T>)source).call();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                EmptySubscription.error(s, ex);
                return;
            }
            
            if (v == null) {
                EmptySubscription.complete(s);
                return;
            }
            
            Iterator<? extends R> it;
            
            try {
                Iterable<? extends R> iter = mapper.apply(v);
                
                it = iter.iterator();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                EmptySubscription.error(s, ex);
                return;
            }
            
            PublisherIterable.subscribe(s, it);
            
            return;
        }
        source.subscribe(new PublisherConcatMapIterableSubscriber<>(s, mapper, prefetch, queueSupplier));
    }
    
    static final class PublisherConcatMapIterableSubscriber<T, R> 
    implements Subscriber<T>, Fuseable.QueueSubscription<R> {
        
        final Subscriber<? super R> actual;
        
        final Function<? super T, ? extends Iterable<? extends R>> mapper;
        
        final int prefetch;
        
        final int limit;
        
        final Supplier<Queue<T>> queueSupplier;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatMapIterableSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherConcatMapIterableSubscriber.class, "wip");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherConcatMapIterableSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherConcatMapIterableSubscriber.class, "requested");
        
        Subscription s;
        
        Queue<T> queue;
        
        volatile boolean done;
        
        volatile boolean cancelled;

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherConcatMapIterableSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherConcatMapIterableSubscriber.class, Throwable.class, "error");

        Iterator<? extends R> current;
        
        int consumed;
        
        int fusionMode;
        
        public PublisherConcatMapIterableSubscriber(Subscriber<? super R> actual,
                Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch,
                Supplier<Queue<T>> queueSupplier) {
            this.actual = actual;
            this.mapper = mapper;
            this.prefetch = prefetch;
            this.queueSupplier = queueSupplier;
            this.limit = prefetch - (prefetch >> 2);
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked")
                    Fuseable.QueueSubscription<T> qs = (Fuseable.QueueSubscription<T>) s;
                    
                    int m = qs.requestFusion(Fuseable.ANY);
                    
                    if (m == Fuseable.SYNC) {
                        fusionMode = m;
                        this.queue = qs;
                        done = true;
                        
                        actual.onSubscribe(this);
                        
                        return;
                    } else
                    if (m == Fuseable.ASYNC) {
                        fusionMode = m;
                        this.queue = qs;
                        
                        actual.onSubscribe(this);
                        
                        s.request(prefetch);
                        return;
                    }
                }
                
                try {
                    queue = queueSupplier.get();
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    s.cancel();
                    EmptySubscription.error(actual, ex);
                    return;
                }

                actual.onSubscribe(this);
                
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (fusionMode != Fuseable.ASYNC) {
                if (!queue.offer(t)) {
                    onError(new IllegalStateException("Queue is full?!"));
                    return;
                }
            }
            drain();
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
                    queue.clear();
                }
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            final Subscriber<? super R> a = actual;
            final Queue<T> q = queue;
            final boolean replenish = fusionMode != Fuseable.SYNC;
            
            int missed = 1;
            
            Iterator<? extends R> it = current;

            for (;;) {

                if (it == null) {
                    
                    boolean d = done;
                    
                    T t;
                    
                    t = q.poll();
                    
                    boolean empty = t == null;
                    
                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }

                    if (t != null) {
                        Iterable<? extends R> iterable;
                        
                        boolean b;
                        
                        try {
                            iterable = mapper.apply(t);
    
                            it = iterable.iterator();
                            
                            b = it.hasNext();
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            onError(ex);
                            it = null;
                            continue;
                        }
                        
                        if (!b) {
                            it = null;
                            consumedOne(replenish);
                            continue;
                        }
                        
                        current = it;
                    }
                }
                
                if (it != null) {
                    long r = requested;
                    long e = 0L;
                    
                    while (e != r) {
                        if (checkTerminated(done, false, a, q)) {
                            return;
                        }

                        R v;
                        
                        try {
                            v = it.next();
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            onError(ex);
                            continue;
                        }
                        
                        a.onNext(v);

                        if (checkTerminated(done, false, a, q)) {
                            return;
                        }

                        e++;
                        
                        boolean b;
                        
                        try {
                            b = it.hasNext();
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            onError(ex);
                            continue;
                        }
                        
                        if (!b) {
                            consumedOne(replenish);
                            it = null;
                            current = null;
                            break;
                        }
                    }
                    
                    if (e == r) {
                        boolean d = done;
                        boolean empty;
                        
                        try {
                            empty = q.isEmpty() && it == null;
                        } catch (Throwable ex) {
                            ExceptionHelper.throwIfFatal(ex);
                            onError(ex);
                            empty = true;
                        }
                        
                        if (checkTerminated(d, empty, a, q)) {
                            return;
                        }
                    }
                    
                    if (e != 0L) {
                        if (r != Long.MAX_VALUE) {
                            REQUESTED.addAndGet(this, -e);
                        }
                    }
                    
                    if (it == null) {
                        continue;
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void consumedOne(boolean enabled) {
            if (enabled) {
                int c = consumed + 1;
                if (c == limit) {
                    consumed = 0;
                    s.request(c);
                } else {
                    consumed = c;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                current = null;
                q.clear();
                return true;
            }
            if (d) {
                if (error != null) {
                    Throwable e = ExceptionHelper.terminate(ERROR, this);
                    
                    current = null;
                    q.clear();

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
        
        @Override
        public void clear() {
            current = null;
            queue.clear();
        }
        
        @Override
        public boolean isEmpty() {
            Iterator<? extends R> it = current;
            if (it != null) {
                return it.hasNext();
            }
            return queue.isEmpty(); // estimate
        }
        
        @Override
        public R poll() {
            Iterator<? extends R> it = current;
            for (;;) {
                if (it == null) {
                    T v = queue.poll();
                    if (v == null) {
                        return null;
                    }
                    
                    it = mapper.apply(v).iterator();
                    
                    if (!it.hasNext()) {
                        continue;
                    }
                    current = it;
                }
                
                R r = it.next();
                
                if (!it.hasNext()) {
                    current = null;
                }
                
                return r;
            }
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            if ((requestedMode & SYNC) != 0 && fusionMode == SYNC) {
                return SYNC;
            }
            return NONE;
        }
        
        @Override
        public int size() {
            return queue.size(); // estimate
        }
    }
}
