package rsc.parallel;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.util.BackpressureHelper;
import rsc.util.SubscriptionHelper;

/**
 * Dispatches the values from upstream in a round robin fashion to subscribers which are
 * ready to consume elements. A value from upstream is sent to only one of the subscribers.
 *
 * @param <T> the value type
 */
public final class ParallelUnorderedSource<T> extends ParallelPublisher<T> {
    final Publisher<? extends T> source;
    
    final int parallelism;
    
    final int prefetch;
    
    final Supplier<Queue<T>> queueSupplier;

    public ParallelUnorderedSource(Publisher<? extends T> source, int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
        this.source = source;
        this.parallelism = parallelism;
        this.prefetch = prefetch;
        this.queueSupplier = queueSupplier;
    }
    
    @Override
    public int parallelism() {
        return parallelism;
    }
    
    @Override
    public boolean ordered() {
        return false;
    }
    
    @Override
    public void subscribe(Subscriber<? super T>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        source.subscribe(new ParallelDispatcher<>(subscribers, prefetch, queueSupplier));
    }
    
    static final class ParallelDispatcher<T> implements Subscriber<T> {

        final Subscriber<? super T>[] subscribers;
        
        final AtomicLongArray requests;

        final long[] emissions;

        final int prefetch;
        
        final int limit;
        
        Subscription s;
        
        final Queue<T> queue;
        
        Throwable error;
        
        volatile boolean done;
        
        int index;
        
        volatile boolean cancelled;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ParallelDispatcher> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ParallelDispatcher.class, "wip");
        
        int produced;
        
        public ParallelDispatcher(Subscriber<? super T>[] subscribers, int prefetch, Supplier<Queue<T>> queueSupplier) {
            this.subscribers = subscribers;
            this.queue = queueSupplier.get();
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.requests = new AtomicLongArray(subscribers.length);
            this.emissions = new long[subscribers.length];
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                int n = subscribers.length;
                
                for (int i = 0; i < n; i++) {
                    int j = i;
                    
                    subscribers[i].onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {
                            if (SubscriptionHelper.validate(n)) {
                                AtomicLongArray ra = requests;
                                for (;;) {
                                    long r = ra.get(j);
                                    if (r == Long.MAX_VALUE) {
                                        return;
                                    }
                                    long u = BackpressureHelper.addCap(r, n);
                                    if (ra.compareAndSet(j, r, u)) {
                                        break;
                                    }
                                }
                                drain();
                            }
                        }
                        
                        @Override
                        public void cancel() {
                            ParallelDispatcher.this.cancel();
                        }
                    });
                }
                
                s.request(prefetch);
            }
        }

        @Override
        public void onNext(T t) {
            if (!queue.offer(t)) {
                cancel();
                onError(new IllegalStateException("Queue is full?"));
                return;
            }
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }
        
        void cancel() {
            if (!cancelled) {
                cancelled = true;
                this.s.cancel();
                
                if (WIP.getAndIncrement(this) == 0) {
                    queue.clear();
                }
            }
        }
        
        T last;
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            
            Queue<T> q = queue;
            Subscriber<? super T>[] a = this.subscribers;
            AtomicLongArray r = this.requests;
            long[] e = this.emissions;
            int n = e.length;
            int idx = index;
            int consumed = produced;
            
            for (;;) {

                int notReady = 0;
                
                for (;;) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }
                    
                    boolean d = done;
                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();
                            for (Subscriber<? super T> s : a) {
                                s.onError(ex);
                            }
                            return;
                        }
                    }

                    boolean empty = q.isEmpty();
                    
                    if (d && empty) {
                        for (Subscriber<? super T> s : a) {
                            s.onComplete();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }
                    
                    long ridx = r.get(idx);
                    long eidx = e[idx];
                    if (ridx != eidx) {

                        T v = q.poll();
                        
                        // FIXME fused queues may return null even if isEmpty is false
                        if (v == null) {
                            System.out.println("??? " + d + " " + empty + " " + ridx + " " + eidx + "; " + last);
                            try {
                                Thread.sleep(50000);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        }
                        
                        last = v;
                        
                        a[idx].onNext(v);
                        
                        e[idx] = eidx + 1;
                        
                        int c = ++consumed;
                        if (c == limit) {
                            consumed = 0;
                            s.request(c);
                        }
                        notReady = 0;
                    } else {
                        notReady++;
                    }
                    
                    idx++;
                    if (idx == n) {
                        idx = 0;
                    }
                    
                    if (notReady == n) {
                        break;
                    }
                }
                
//                int w = wip;
//                if (w == missed) {
                    index = idx;
                    produced = consumed;
                    missed = WIP.addAndGet(this, -missed);
                    if (missed == 0) {
                        break;
                    }
//                } else {
//                    missed = w;
//                }
            }
        }
    }
}
