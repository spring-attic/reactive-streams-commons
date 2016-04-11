package reactivestreams.commons.publisher;

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
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.DeferredSubscription;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Splits the source sequence into potentially overlapping windows whose boundaries
 * are determined by other publishers.
 * 
 * <p>
 * This implementation uses a fixed buffer and open windows go in lockstep.
 *
 * @param <T> the source value type
 * @param <U> the window-start indicator values
 * @param <V> the window-end indicator values
 */
public final class PublisherWindowBeginEnd<T, U, V> extends PublisherSource<T, PublisherBase<T>> {

    final Publisher<U> windowBegin;
    
    final Function<? super U, ? extends Publisher<V>> windowEnd;
    
    final Supplier<? extends Queue<T>> queueSupplier;
    
    final int bufferSize;
    
    public PublisherWindowBeginEnd(
            Publisher<? extends T> source, 
            Publisher<U> windowBegin,
            Function<? super U, ? extends Publisher<V>> windowEnd, 
            Supplier<? extends Queue<T>> queueSupplier, 
                    int bufferSize) {
        super(source);
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize > 0 required but it was " + bufferSize);
        }
        this.windowBegin = Objects.requireNonNull(windowBegin, "windowBegin");
        this.windowEnd = Objects.requireNonNull(windowEnd, "windowEnd");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }

    @Override
    public void subscribe(Subscriber<? super PublisherBase<T>> s) {
        
        Queue<T> q;
        
        try {
            q = queueSupplier.get();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
            return;
        }
        
        WindowBeginEndMainSubscriber<T, U, V> main = new WindowBeginEndMainSubscriber<>(s, windowEnd, bufferSize, q);
        
        s.onSubscribe(main);
        
        windowBegin.subscribe(main.begin);
        
        source.subscribe(main);
    }
    
    static final class WindowBeginEndMainSubscriber<T, U, V>
    implements Subscriber<T>, Subscription {
        
        final Subscriber<? super PublisherBase<T>> actual;
        
        final Function<? super U, ? extends Publisher<V>> windowEnd;
        
        final int bufferSize;
        
        final int limit;
        
        final Queue<T> queue;
        
        final WindowBeginEndBegin<U> begin;
        
        volatile Subscription main;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<WindowBeginEndMainSubscriber, Subscription> MAIN =
                AtomicReferenceFieldUpdater.newUpdater(WindowBeginEndMainSubscriber.class, Subscription.class, "main");

        volatile PublisherWindowInner<T, V>[] windows;
        
        @SuppressWarnings("rawtypes")
        static final PublisherWindowInner[] EMPTY = new PublisherWindowInner[0];
        @SuppressWarnings("rawtypes")
        static final PublisherWindowInner[] TERMINATED = new PublisherWindowInner[0];
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<WindowBeginEndMainSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(WindowBeginEndMainSubscriber.class, "wip");
        
        volatile int open;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<WindowBeginEndMainSubscriber> OPEN =
                AtomicIntegerFieldUpdater.newUpdater(WindowBeginEndMainSubscriber.class, "open");
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<WindowBeginEndMainSubscriber> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(WindowBeginEndMainSubscriber.class, "once");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<WindowBeginEndMainSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(WindowBeginEndMainSubscriber.class, Throwable.class, "error");

        volatile boolean done;
        
        long produced;
        
        @SuppressWarnings("unchecked")
        public WindowBeginEndMainSubscriber(
                Subscriber<? super PublisherBase<T>> subscriber,
                Function<? super U, ? extends Publisher<V>> windowEnd, 
                        int bufferSize, 
                        Queue<T> queue) {
            this.actual = subscriber;
            this.windowEnd = windowEnd;
            this.bufferSize = bufferSize;
            this.queue = queue;
            this.limit = bufferSize - (bufferSize >> 2);
            this.begin = new WindowBeginEndBegin<>(this);
            this.open = 1;
            this.windows = EMPTY;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(MAIN, this, s)) {
                s.request(bufferSize);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (!queue.offer(t)) {
                SubscriptionHelper.terminate(MAIN, this);
                onError(new IllegalStateException("Queue is full?"));
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
        public void request(long n) {
            begin.request(n);
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                if (OPEN.decrementAndGet(this) == 0) {
                    cancelAll();
                }
            }
        }
        
        void begin(U u) {
            Publisher<V> p;

            try {
                p = windowEnd.apply(u);
            } catch (Throwable ex) {
                begin.cancel();
                ExceptionHelper.throwIfFatal(ex);
                error(ExceptionHelper.unwrap(ex));
                return;
            }
            
            if (p == null) {
                begin.cancel();
                
                error(new NullPointerException("The windowEnd returned a null publisher"));
                return;
            }
            
            OPEN.getAndIncrement(this);
            
            PublisherWindowInner<T, V> inner = new PublisherWindowInner<>(this);
            
            if (add(inner)) {
                actual.onNext(inner);
                
                p.subscribe(inner);
            }
        }
        
        void error(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void complete() {
            done = true;
            drain();
        }
        
        boolean add(PublisherWindowInner<T, V> inner) {
            if (windows == TERMINATED) {
                return false;
            }
            
            synchronized (this) {
                if (windows == TERMINATED) {
                    return false;
                }

                PublisherWindowInner<T, V>[] a = windows;
                int n = a.length;
                
                @SuppressWarnings("unchecked")
                PublisherWindowInner<T, V>[] b = new PublisherWindowInner[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;
                
                windows = b;
                return true;
            }
        }
        
        @SuppressWarnings("unchecked")
        void remove(PublisherWindowInner<T, V> inner) {
            if (windows == TERMINATED) {
                return;
            }
            
            synchronized (this) {
                PublisherWindowInner<T, V>[] a = windows;
                if (a == TERMINATED) {
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
                
                if (n == 1) {
                    windows = EMPTY;
                    return;
                }
                
                PublisherWindowInner<T, V>[] b = new PublisherWindowInner[n - 1];
                
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
                
                windows = b;
            }
            inner.cancelEnd();
        }
        
        @SuppressWarnings("unchecked")
        void terminate() {
            if (windows == TERMINATED) {
                return;
            }
            
            PublisherWindowInner<T, V>[] a;
            synchronized (this) {
                a = windows;
                if (a == TERMINATED) {
                    return;
                }
                windows = TERMINATED;
            }
            for (PublisherWindowInner<T, V> inner : a) {
                inner.cancelEnd();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            
            final Queue<T> q = queue;

            for (;;) {
                
                long e = 0L;
                
                for (;;) {
                    PublisherWindowInner<T, V>[] a = windows;
                    int n = a.length;
                    
                    if (n != 0) {
                        
                        long r = Long.MAX_VALUE;
                        
                        for (PublisherWindowInner<T, V> inner : a) {
                            if (inner.done) {
                                remove(inner);
                                inner.actualComplete();
                                n--;
                            } else {
                                r = Math.min(r, inner.requested);
                            }
                        }
                        
                        if (r == 0L) {
                            break;
                        }
                        
                        if (n != 0) {
                            boolean d = done;
                            T v = q.peek();
                            boolean empty = v == null;
                            
                            if (open == 0) {
                                q.clear();
                                return;
                            } else
                            if (d) {
                                Throwable ex = ExceptionHelper.terminate(ERROR, this);
                                if (ex != null && ex != ExceptionHelper.TERMINATED) {
                                    q.clear();
                                    
                                    cancelAll();
                                    
                                    for (PublisherWindowInner<T, V> inner : a) {
                                        if (!inner.done) {
                                            inner.actualError(ex);
                                        }
                                    }
                                    
                                    actual.onError(ex);
                                    
                                    return;
                                } else
                                if (empty) {
                                    cancelAll();

                                    for (PublisherWindowInner<T, V> inner : a) {
                                        if (!inner.done) {
                                            inner.actualComplete();
                                        }
                                    }
                                    
                                    actual.onComplete();
                                    
                                    return;
                                }
                            }
                            
                            if (empty) {
                                break;
                            }
                            
                            for (PublisherWindowInner<T, V> inner : a) {
                                if (!inner.done) {
                                    inner.actualNext(v);
                                    inner.producedOne();
                                }
                            }
                            
                            q.poll();
                            e++;
                        }
                    }
                    
                    if (n == 0) {
                        if (done) {
                            Throwable ex = ExceptionHelper.terminate(ERROR, this);
                            if (ex != null && ex != ExceptionHelper.TERMINATED) {
                                q.clear();
                                
                                cancelAll();
                                
                                actual.onError(ex);
                                
                                return;
                            } else
                            if (q.isEmpty()) {
                                cancelAll();

                                actual.onComplete();
                                
                                return;
                            }
                        }
                        break;
                    }
                }
                
                if (e != 0L) {
                    requestMore(e);
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void requestMore(long e) {
            long p = BackpressureHelper.addCap(produced, e);
            if (p >= limit) {
                produced = 0L;
                main.request(p);
            } else {
                produced = p;
            }
        }
        
        void innerDone(PublisherWindowInner<T, V> inner) {
            if (OPEN.decrementAndGet(this) == 0) {
                cancelAll();
            } else {
                remove(inner);
            }
        }
        
        void cancelAll() {
            SubscriptionHelper.terminate(MAIN, this);
            begin.cancel();
            terminate();
        }
        
        void end(PublisherWindowInner<T, V> ender) {
            drain();
        }
        
        void endError(PublisherWindowInner<T, V> ender, Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
    }
    
    static final class WindowBeginEndBegin<U>
    extends DeferredSubscription
    implements Subscriber<U> {
        
        final WindowBeginEndMainSubscriber<?, U, ?> parent;

        public WindowBeginEndBegin(WindowBeginEndMainSubscriber<?, U, ?> parent) {
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }
        
        @Override
        public void onNext(U t) {
            parent.begin(t);
        }
        
        @Override
        public void onError(Throwable t) {
            parent.error(t);
        }
        
        @Override
        public void onComplete() {
            parent.complete();
        }
    }
    
    static final class PublisherWindowInner<T, V>
    extends PublisherBase<T>
    implements Subscriber<V>, Subscription {
        
        final WindowBeginEndMainSubscriber<T, ?, V> parent;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowInner.class, "once");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherWindowInner> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherWindowInner.class, "requested");

        volatile Subscriber<? super T> actual;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowInner, Subscriber> ACTUAL =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowInner.class, Subscriber.class, "actual");

        static final TerminatedSubscriber TERMINATED = new TerminatedSubscriber();
        
        volatile boolean cancelled;
        
        volatile boolean done;
        Throwable error;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowInner.class, Subscription.class, "s");

        public PublisherWindowInner(WindowBeginEndMainSubscriber<T, ?, V> parent) {
            this.parent = parent;
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                s.onSubscribe(this);
                if (!ACTUAL.compareAndSet(this, null, s)) {
                    Throwable e = error;
                    if (e != null) {
                        s.onError(e);
                    } else {
                        s.onComplete();
                    }
                    return;
                }
                if (cancelled) {
                    actual = null;
                } else {
                    parent.drain();
                }
            } else {
                EmptySubscription.error(s, new IllegalStateException("This publisher allows only a single subscription"));
            }
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
                parent.drain();
            }
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                actual = null;
                SubscriptionHelper.terminate(S, this);
                parent.innerDone(this);
            }
        }
        
        void producedOne() {
            REQUESTED.decrementAndGet(this);
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(V t) {
            if (done) {
                return;
            }
            done = true;
            SubscriptionHelper.terminate(S, this);
            
            parent.end(this);
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            
            parent.endError(this, t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            parent.end(this);
        }
        
        void actualComplete() {
            if (!ACTUAL.compareAndSet(this, null, TERMINATED)) {
                Subscriber<? super T> a = actual;
                if (a != null) {
                    a.onComplete();
                }
            }
        }
        
        void actualError(Throwable e) {
            error = e;
            if (!ACTUAL.compareAndSet(this, null, TERMINATED)) {
                Subscriber<? super T> a = actual;
                if (a != null) {
                    a.onError(e);
                }
            }
        }
        
        void actualNext(T value) {
            Subscriber<? super T> a = actual;
            if (a != null) {
                a.onNext(value);
            }
        }
        
        void cancelEnd() {
            SubscriptionHelper.terminate(S, this);
        }
    }
    
    static final class TerminatedSubscriber implements Subscriber<Object> {

        @Override
        public void onSubscribe(Subscription s) {
            // irrelevant
        }

        @Override
        public void onNext(Object t) {
            // irrelevant
        }

        @Override
        public void onError(Throwable t) {
            // irrelevant
        }

        @Override
        public void onComplete() {
            // irrelevant
        }
        
    }
}
