package reactivestreams.commons.publisher;

import java.util.Iterator;
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

import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 */
public final class PublisherConcatMapIterable<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends Iterable<? extends R>> mapper;
    
    final int prefetch;
    
    final Supplier<Queue<T>> queueSupplier;
    
    public PublisherConcatMapIterable(Publisher<? extends T> source,
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



    @Override
    public void subscribe(Subscriber<? super R> s) {
        source.subscribe(new PublisherConcatMapIterableSubscriber<>(s, mapper, prefetch, queueSupplier));
    }
    
    static final class PublisherConcatMapIterableSubscriber<T, R> implements Subscriber<T>, Subscription {
        
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
    }
}
