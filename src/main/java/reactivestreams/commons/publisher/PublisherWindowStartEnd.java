package reactivestreams.commons.publisher;

import java.util.HashSet;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.subscription.DeferredSubscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.BackpressureHelper;
import reactivestreams.commons.support.ExceptionHelper;
import reactivestreams.commons.support.SubscriptionHelper;
import reactivestreams.commons.support.UnsignalledExceptions;

/**
 * Splits the source sequence into potentially overlapping windowEnds controlled by items of a 
 * start Publisher and end Publishers derived from the start values.
 *
 * @param <T> the source value type
 * @param <U> the window starter value type
 * @param <V> the window end value type (irrelevant)
 */
public final class PublisherWindowStartEnd<T, U, V> extends PublisherSource<T, PublisherBase<T>>{

    final Publisher<U> start;
    
    final Function<? super U, ? extends Publisher<V>> end;
    
    final Supplier<? extends Queue<Object>> drainQueueSupplier;
    
    final Supplier<? extends Queue<T>> processorQueueSupplier;

    public PublisherWindowStartEnd(Publisher<? extends T> source, Publisher<U> start,
            Function<? super U, ? extends Publisher<V>> end, Supplier<? extends Queue<Object>> drainQueueSupplier,
            Supplier<? extends Queue<T>> processorQueueSupplier) {
        super(source);
        this.start = Objects.requireNonNull(start, "start");
        this.end = Objects.requireNonNull(end, "end");
        this.drainQueueSupplier = Objects.requireNonNull(drainQueueSupplier, "drainQueueSupplier");
        this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super PublisherBase<T>> s) {

        Queue<Object> q;
        
        try {
            q = drainQueueSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The drainQueueSupplier returned a null queue"));
            return;
        }
        
        PublisherWindowStartEndMain<T, U, V> main = new PublisherWindowStartEndMain<>(s, q, end, processorQueueSupplier);
        
        s.onSubscribe(main);
        
        start.subscribe(main.starter);
        
        source.subscribe(main);
    }
    
    static final class PublisherWindowStartEndMain<T, U, V> 
    implements Subscriber<T>, Subscription, Runnable {
        
        final Subscriber<? super PublisherBase<T>> actual;
        
        final Queue<Object> queue;
        
        final PublisherWindowStartEndStarter<T, U, V> starter;
        
        final Function<? super U, ? extends Publisher<V>> end;
        
        final Supplier<? extends Queue<T>> processorQueueSupplier;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherWindowStartEndMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowStartEndMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, "wip");
        
        volatile boolean cancelled;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowStartEndMain, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, Subscription.class,  "s");
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowStartEndMain> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, "once");
        
        volatile int open;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowStartEndMain> OPEN =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, "open");
        
        Set<PublisherWindowStartEndEnder<T, V>> windowEnds;
        
        Set<UnicastProcessor<T>> windows;

        volatile boolean mainDone;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWindowStartEndMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWindowStartEndMain.class, Throwable.class,  "error");

        public PublisherWindowStartEndMain(Subscriber<? super PublisherBase<T>> actual, Queue<Object> queue,
                Function<? super U, ? extends Publisher<V>> end,
                Supplier<? extends Queue<T>> processorQueueSupplier) {
            this.actual = actual;
            this.queue = queue;
            this.starter = new PublisherWindowStartEndStarter<>(this);
            this.end = end;
            this.windowEnds = new HashSet<>();
            this.windows = new HashSet<>();
            this.processorQueueSupplier = processorQueueSupplier;
            this.open = 1;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            synchronized (this) {
                queue.offer(t);
            }
            drain();
        }
        
        @Override
        public void onError(Throwable t) {
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            closeMain();
            starter.cancel();
            mainDone = true;
            
            drain();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
            }
        }
        
        @Override
        public void cancel() {
            cancelled = true;
            
            starter.cancel();
            closeMain();
        }
        
        void starterNext(U u) {
            NewWindow<U> nw = new NewWindow<>(u);
            synchronized (this) {
                queue.offer(nw);
            }
            drain();
        }
        
        void starterError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void starterComplete() {
            closeMain();
            drain();
        }
        
        void endSignal(PublisherWindowStartEndEnder<T, V> end) {
            remove(end);
            synchronized (this) {
                queue.offer(end);
            }
            drain();
        }
        
        void endError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void closeMain() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                run();
            }
        }
        
        @Override
        public void run() {
            if (OPEN.decrementAndGet(this) == 0) {
                SubscriptionHelper.terminate(S, this);
            }
        }
        
        boolean add(PublisherWindowStartEndEnder<T, V> ender) {
            synchronized (starter) {
                Set<PublisherWindowStartEndEnder<T, V>> set = windowEnds;
                if (set != null) {
                    set.add(ender);
                    return true;
                }
            }
            ender.cancel();
            return false;
        }
        
        void remove(PublisherWindowStartEndEnder<T, V> ender) {
            synchronized (starter) {
                Set<PublisherWindowStartEndEnder<T, V>> set = windowEnds;
                if (set != null) {
                    set.remove(ender);
                }
            }
        }
        
        void removeAll() {
            Set<PublisherWindowStartEndEnder<T, V>> set;
            synchronized (starter) {
                set = windowEnds;
                if (set == null) {
                    return;
                }
                windowEnds = null;
            }
            
            for (Subscription s : set) {
                s.cancel();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            final Subscriber<? super UnicastProcessor<T>> a = actual;
            final Queue<Object> q = queue;
            
            int missed = 1;
            
            for (;;) {
                
                for (;;) {
                    Throwable e = error;
                    if (e != null) {
                        e = ExceptionHelper.terminate(ERROR, this);
                        if (e != ExceptionHelper.TERMINATED) {
                            SubscriptionHelper.terminate(S, this);
                            starter.cancel();
                            removeAll();

                            for (UnicastProcessor<T> w : windows) {
                                w.onError(e);
                            }
                            windows = null;
                            
                            q.clear();
                            
                            a.onError(e);
                        }
                        
                        return;
                    }
                    
                    if (mainDone || open == 0) {
                        removeAll();

                        for (UnicastProcessor<T> w : windows) {
                            w.onComplete();
                        }
                        windows = null;
                        
                        a.onComplete();
                        return;
                    }
                    
                    Object o = q.poll();
                    
                    if (o == null) {
                        break;
                    }
                    
                    if (o instanceof NewWindow) {
                        if (!cancelled && open != 0 && !mainDone) {
                            @SuppressWarnings("unchecked")
                            NewWindow<U> newWindow = (NewWindow<U>) o;
                            
                            Queue<T> pq;
                            
                            try {
                                pq = processorQueueSupplier.get();
                            } catch (Throwable ex) {
                                ExceptionHelper.addThrowable(ERROR, this, ex);
                                continue;
                            }
                            
                            if (pq == null) {
                                ExceptionHelper.addThrowable(ERROR, this, new NullPointerException("The processorQueueSupplier returned a null queue"));
                                continue;
                            }
                            
                            Publisher<V> p;
                            
                            try {
                                p = end.apply(newWindow.value);
                            } catch (Throwable ex) {
                                ExceptionHelper.throwIfFatal(ex);
                                ExceptionHelper.addThrowable(ERROR, this, ExceptionHelper.unwrap(ex));
                                continue;
                            }

                            if (p == null) {
                                ExceptionHelper.addThrowable(ERROR, this, new NullPointerException("The end returned a null publisher"));
                                continue;
                            }

                            OPEN.getAndIncrement(this);
                            
                            UnicastProcessor<T> w = new UnicastProcessor<>(pq, this);
                            
                            PublisherWindowStartEndEnder<T, V> end = new PublisherWindowStartEndEnder<>(this, w);
                            
                            windows.add(w);
                            
                            if (add(end)) {
                                
                                long r = requested;
                                if (r != 0L) {
                                    a.onNext(w);
                                    if (r != Long.MAX_VALUE) {
                                        REQUESTED.decrementAndGet(this);
                                    }
                                } else {
                                    ExceptionHelper.addThrowable(ERROR, this, new IllegalStateException("Could not emit window due to lack of requests"));
                                    continue;
                                }
                                
                                p.subscribe(end);
                            }
                        }
                    } else
                    if (o instanceof PublisherWindowStartEndEnder) {
                        @SuppressWarnings("unchecked")
                        PublisherWindowStartEndEnder<T, V> end = (PublisherWindowStartEndEnder<T, V>) o;
                        
                        end.window.onComplete();
                    } else {
                        @SuppressWarnings("unchecked")
                        T v = (T)o;
                        
                        for (UnicastProcessor<T> w : windows) {
                            w.onNext(v);
                        }
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
    
    static final class PublisherWindowStartEndStarter<T, U, V>
    extends DeferredSubscription
    implements Subscriber<U> {

        final PublisherWindowStartEndMain<T, U, V> main;
        
        public PublisherWindowStartEndStarter(PublisherWindowStartEndMain<T, U, V> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(U t) {
            main.starterNext(t);
        }

        @Override
        public void onError(Throwable t) {
            main.starterError(t);
        }

        @Override
        public void onComplete() {
            main.starterComplete();
        }
        
    }
    
    static final class PublisherWindowStartEndEnder<T, V> 
    extends DeferredSubscription
    implements Subscriber<V> {

        final PublisherWindowStartEndMain<T, ?, V> main;
        
        final UnicastProcessor<T> window;
        
        public PublisherWindowStartEndEnder(PublisherWindowStartEndMain<T, ?, V> main, UnicastProcessor<T> window) {
            this.main = main;
            this.window = window;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(V t) {
            cancel();
            
            main.endSignal(this);
        }

        @Override
        public void onError(Throwable t) {
            main.endError(t);
        }

        @Override
        public void onComplete() {
            main.endSignal(this);
        }
        
    }
    
    static final class NewWindow<U> {
        final U value;
        public NewWindow(U value) {
            this.value = value;
        }
    }
}
