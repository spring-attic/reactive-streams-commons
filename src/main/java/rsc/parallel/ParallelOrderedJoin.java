package rsc.parallel;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.publisher.Px;
import rsc.util.BackpressureHelper;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Merges the individual 'rails' of the source ParallelPublisher, ordered,
 * into a single regular Publisher sequence (exposed as Px).
 *
 * @param <T> the value type
 */
public final class ParallelOrderedJoin<T> extends Px<T> {
    final ParallelOrderedBase<T> source;
    final int prefetch;
    final Supplier<Queue<OrderedItem<T>>> queueSupplier;
    
    public ParallelOrderedJoin(ParallelOrderedBase<T> source, int prefetch, Supplier<Queue<OrderedItem<T>>> queueSupplier) {
        this.source = source;
        this.prefetch = prefetch;
        this.queueSupplier = queueSupplier;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        JoinSubscription<T> parent = new JoinSubscription<>(s, source.parallelism(), prefetch, queueSupplier);
        s.onSubscribe(parent);
        source.subscribeOrdered(parent.subscribers);
    }
    
    static final class JoinSubscription<T> implements Subscription {
        final Subscriber<? super T> actual;
        
        final JoinInnerSubscriber<T>[] subscribers;
        
        final OrderedItem<T>[] peek;
        static final OrderedItem<Object> FINISHED = PrimaryOrderedItem.of(null, Long.MAX_VALUE);
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<JoinSubscription, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(JoinSubscription.class, Throwable.class, "error");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<JoinSubscription> WIP =
                AtomicIntegerFieldUpdater.newUpdater(JoinSubscription.class, "wip");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<JoinSubscription> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(JoinSubscription.class, "requested");
        
        volatile boolean cancelled;

        @SuppressWarnings("unchecked")
        public JoinSubscription(Subscriber<? super T> actual, int n, int prefetch, Supplier<Queue<OrderedItem<T>>> queueSupplier) {
            this.actual = actual;
            JoinInnerSubscriber<T>[] a = new JoinInnerSubscriber[n];
            
            for (int i = 0; i < n; i++) {
                a[i] = new JoinInnerSubscriber<>(this, i, prefetch, queueSupplier);
            }
            
            this.subscribers = a;
            this.peek = new OrderedItem[n];
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
                for (JoinInnerSubscriber<T> s : subscribers) {
                    s.cancel();
                }
                
                if (WIP.getAndIncrement(this) == 0) {
                    cleanup();
                }
            }
        }
        
        void cleanup() {
            for (JoinInnerSubscriber<T> s : subscribers) {
                s.queue.clear();; 
            }
        }
        
        void onError(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            drainLoop();
        }
        
        @SuppressWarnings("unchecked")
        void drainLoop() {
            int missed = 1;
            
            JoinInnerSubscriber<T>[] s = this.subscribers;
            int n = s.length;
            Subscriber<? super T> a = this.actual;
            OrderedItem<T>[] peek = this.peek;
            
            for (;;) {

                long r = requested;
                long e = 0;
                
                for (;;) {
                    if (cancelled) {
                        Arrays.fill(peek, null);
                        cleanup();
                        return;
                    }
                    
                    Throwable ex = error;
                    if (ex != null) {
                        ex = ExceptionHelper.terminate(ERROR, this);
                        Arrays.fill(peek, null);
                        cleanup();

                        a.onError(ex);
                        return;
                    }
                    
                    boolean fullRow = true;
                    int finished = 0;
                    
                    OrderedItem<T> min = null;
                    int minIndex = -1;
                    
                    for (int i = 0; i < n; i++) {
                        JoinInnerSubscriber<T> inner = s[i];
                        OrderedItem<T> p = peek[i];
                        if (p == null) {
                            boolean d = inner.done;
                            OrderedItem<T> v = inner.queue.poll();
                            boolean empty = v == null;
                            
                            if (d && empty) {
                                p = (OrderedItem<T>)FINISHED;
                                peek[i] = p;
                                finished++;
                            } else 
                            if (empty) {
                                fullRow = false;
                                break;
                            } else {
                                peek[i] = v;
                                p = v;
                            }
                        } else 
                        if (p == FINISHED) {
                            finished++;
                        }
                        
                        if (min == null || min.compareTo(p) > 0) {
                            min = p;
                            minIndex = i;
                        }
                    }
                    
                    if (finished == n) {
                        a.onComplete();
                        return;
                    }
                    
                    if (!fullRow || min == null || e == r) {
                        break;
                    }
                    
                    
                    peek[minIndex] = null;

                    a.onNext(min.get());
                    
                    s[minIndex].requestOne();
                    e++;
                }
                
                if (e != 0 && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }
                
                int w = wip;
                if (w == missed) {
                    missed = WIP.addAndGet(this, -missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }
    
    static final class JoinInnerSubscriber<T> implements Subscriber<OrderedItem<T>> {
        
        final JoinSubscription<T> parent;
        
        final int prefetch;
        
        final int limit;
        
        final int index;

        final Queue<OrderedItem<T>> queue;

        long produced;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<JoinInnerSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(JoinInnerSubscriber.class, Subscription.class, "s");
        
        volatile boolean done;
        
        public JoinInnerSubscriber(JoinSubscription<T> parent, int index, int prefetch, Supplier<Queue<OrderedItem<T>>> queueSupplier) {
            this.index = index;
            this.parent = parent;
            this.prefetch = prefetch ;
            this.limit = prefetch - (prefetch >> 2);
            this.queue = queueSupplier.get();
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(OrderedItem<T> t) {
            if (!queue.offer(t)) {
                cancel();
                parent.onError(new IllegalStateException("Queue is full?!"));
                return;
            }
            parent.drain();
        }
        
        @Override
        public void onError(Throwable t) {
            parent.onError(t);
        }
        
        @Override
        public void onComplete() {
            this.done = true;
            parent.drain();
        }
        
        public void requestOne() {
            long p = produced + 1;
            if (p == limit) {
                produced = 0;
                s.request(p);
            } else {
                produced = p;
            }
        }

        public void request(long n) {
            long p = produced + n;
            if (p >= limit) {
                produced = 0;
                s.request(p);
            } else {
                produced = p;
            }
        }

        public void cancel() {
            SubscriptionHelper.terminate(S, this);
        }
    }
}
