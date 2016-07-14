package rsc.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.documentation.*;
import rsc.flow.*;

import rsc.subscriber.SubscriberState;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Combines the latest values from multiple sources through a function.
 *
 * @param <T> the value type of the sources
 * @param <R> the result type
 */
@FusionSupport(input = { FusionMode.NOT_APPLICABLE}, innerInput = { FusionMode.SCALAR }, output = { FusionMode.ASYNC })
public final class PublisherCombineLatest<T, R> 
extends Px<R>
        implements MultiReceiver, Fuseable {

    final Publisher<? extends T>[] array;

    final Iterable<? extends Publisher<? extends T>> iterable;

    final Function<Object[], R> combiner;
    
    final Supplier<? extends Queue<SourceAndArray>> queueSupplier;
    
    final int bufferSize;

    public PublisherCombineLatest(Publisher<? extends T>[] array,
            Function<Object[], R> combiner, Supplier<? extends Queue<SourceAndArray>> queueSupplier,
                    int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
        }

        this.array = Objects.requireNonNull(array, "array");
        this.iterable = null;
        this.combiner = Objects.requireNonNull(combiner, "combiner");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }
    
    public PublisherCombineLatest(Iterable<? extends Publisher<? extends T>> iterable,
            Function<Object[], R> combiner, Supplier<? extends Queue<SourceAndArray>> queueSupplier,
                    int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
        }
        
        this.array = null;
        this.iterable = Objects.requireNonNull(iterable, "iterable");
        this.combiner = Objects.requireNonNull(combiner, "combiner");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }

    @Override
    public Iterator<?> upstreams() {
        return iterable != null ? iterable.iterator() : Arrays.asList(array).iterator();
    }

    @Override
    public long upstreamCount() {
        return array != null ? array.length : -1L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super R> s) {
        Publisher<? extends T>[] a = array;
        int n;
        if (a == null) {
            n = 0;
            a = new Publisher[8];

            Iterator<? extends Publisher<? extends T>> it;

            try {
                it = iterable.iterator();
            } catch (Throwable e) {
                SubscriptionHelper.error(s, e);
                return;
            }

            if (it == null) {
                SubscriptionHelper.error(s, new NullPointerException("The iterator returned is null"));
                return;
            }

            for (;;) {

                boolean b;

                try {
                    b = it.hasNext();
                } catch (Throwable e) {
                    SubscriptionHelper.error(s, e);
                    return;
                }

                if (!b) {
                    break;
                }

                Publisher<? extends T> p;

                try {
                    p = it.next();
                } catch (Throwable e) {
                    SubscriptionHelper.error(s, e);
                    return;
                }

                if (p == null) {
                    SubscriptionHelper.error(s, new NullPointerException("The Publisher returned by the iterator is " +
                      "null"));
                    return;
                }

                if (n == a.length) {
                    Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
                    System.arraycopy(a, 0, c, 0, n);
                    a = c;
                }
                a[n++] = p;
            }

        } else {
            n = a.length;
        }

        if (n == 0) {
            SubscriptionHelper.complete(s);
            return;
        }
        if (n == 1) {
            new PublisherMap<>(a[0], new Function<T, R>() {
                @Override
                public R apply(T t) {
                    return combiner.apply(new Object[] { t });
                }
            }).subscribe(s);
            return;
        }
        
        Queue<SourceAndArray> queue;
        
        try {
            queue = queueSupplier.get();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }
        
        if (queue == null) {
            SubscriptionHelper.error(s, new NullPointerException("The queueSupplier returned a null queue"));
            return;
        }
        
        PublisherCombineLatestCoordinator<T, R> coordinator = 
                new PublisherCombineLatestCoordinator<>(s, combiner, n, queue, bufferSize);
        
        s.onSubscribe(coordinator);
        
        coordinator.subscribe(a, n);
    }
    
    static final class PublisherCombineLatestCoordinator<T, R> 
    implements QueueSubscription<R>, MultiReceiver, SubscriberState {

        final Subscriber<? super R> actual;
        
        final Function<Object[], R> combiner;
        
        final PublisherCombineLatestInner<T>[] subscribers;
        
        final Queue<SourceAndArray> queue;
        
        final Object[] latest;
        
        boolean outputFused;

        int nonEmptySources;
        
        int completedSources;
        
        volatile boolean cancelled;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherCombineLatestCoordinator> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherCombineLatestCoordinator.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherCombineLatestCoordinator> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherCombineLatestCoordinator.class, "wip");
        
        volatile boolean done;
        
        volatile Throwable error;
        
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherCombineLatestCoordinator, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherCombineLatestCoordinator.class, Throwable.class, "error");
        
        public PublisherCombineLatestCoordinator(Subscriber<? super R> actual, 
                Function<Object[], R> combiner, int n, Queue<SourceAndArray> queue,
                int bufferSize) {
            this.actual = actual;
            this.combiner = combiner;
            @SuppressWarnings("unchecked")
            PublisherCombineLatestInner<T>[] a = new PublisherCombineLatestInner[n];
            for (int i = 0; i < n; i++) {
                a[i] = new PublisherCombineLatestInner<>(this, i, bufferSize);
            }
            this.subscribers = a;
            this.latest = new Object[n];
            this.queue = queue;
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
            if (cancelled) {
                return;
            }
            cancelled = true;
            cancelAll();
            
            if (WIP.getAndIncrement(this) == 0) {
                queue.clear();
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public Iterator<?> upstreams() {
            return Arrays.asList(subscribers).iterator();
        }

        @Override
        public long upstreamCount() {
            return subscribers.length;
        }

        void subscribe(Publisher<? extends T>[] sources, int n) {
            PublisherCombineLatestInner<T>[] a = subscribers;
            
            for (int i = 0; i < n; i++) {
                if (done || cancelled) {
                    return;
                }
                sources[i].subscribe(a[i]);
            }
        }
        
        void innerValue(int index, T value) {
            
            boolean replenishInsteadOfDrain;
            
            synchronized (this) {
                Object[] os = latest;

                int localNonEmptySources = nonEmptySources;
                
                if (os[index] == null) {
                    localNonEmptySources++;
                    nonEmptySources = localNonEmptySources;
                }
                
                os[index] = value;

                if (os.length == localNonEmptySources) {
                    SourceAndArray sa = new SourceAndArray(subscribers[index], os.clone());
                    
                    queue.offer(sa);
                    
                    replenishInsteadOfDrain = false;
                } else {
                    replenishInsteadOfDrain = true;
                }
            }
            
            if (replenishInsteadOfDrain) {
                subscribers[index].requestOne();
            } else {
                drain();
            }
        }
        
        void innerComplete(int index) {
            synchronized (this) {
                Object[] os = latest;
                
                if (os[index] != null) {
                    int localCompletedSources = completedSources + 1;
                    
                    if (localCompletedSources == os.length) {
                        done = true;
                    } else {
                        completedSources = localCompletedSources;
                        return;
                    }
                } else {
                    done = true;
                }
            }
            drain();
        }
        
        void innerError(Throwable e) {
            
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void drainOutput() {
            final Subscriber<? super R> a = actual;
            final Queue<SourceAndArray> q = queue;
            
            int missed = 1;
            
            for (;;) {
                
                if (cancelled) {
                    q.clear();
                    return;
                }
                
                Throwable ex = error;
                if (ex != null) {
                    q.clear();
                    
                    a.onError(ex);
                    return;
                }
                
                boolean d = done;
                
                boolean empty = q.isEmpty();
                
                if (!empty) {
                    a.onNext(null);
                }
                
                if (d && empty) {
                    a.onComplete();
                    return;
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void drainAsync() {
            final Subscriber<? super R> a = actual;
            final Queue<SourceAndArray> q = queue;
            
            int missed = 1;
            
            for (;;) {
                
                long r = requested;
                long e = 0L;
                
                while (e != r) {
                    boolean d = done;
                    
                    SourceAndArray v = q.poll();
                    
                    boolean empty = v == null;
                    
                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    R w;
                    
                    try {
                        w = combiner.apply(v.array);
                    } catch (Throwable ex) {
                        innerError(ExceptionHelper.unwrap(ex));
                        ExceptionHelper.throwIfFatal(ex);
                        continue;
                    }
                    
                    if (w == null) {
                        innerError(new NullPointerException("The combiner returned a null value"));
                        continue;
                    }
                    
                    a.onNext(w);
                    
                    v.source.requestOne();
                    
                    e++;
                }
                
                if (e == r) {
                    if (checkTerminated(done, q.isEmpty(), a, q)) {
                        return;
                    }
                }
                
                if (e != 0L && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            if (outputFused) {
                drainOutput();
            } else {
                drainAsync();
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                cancelAll();
                q.clear();
                return true;
            }
            
            if (d) {
                Throwable e = ExceptionHelper.terminate(ERROR, this);
                
                if (e != null && e != ExceptionHelper.TERMINATED) {
                    cancelAll();
                    q.clear();
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    cancelAll();

                    a.onComplete();
                    return true;
                }
            }
            return false;
        }
        
        void cancelAll() {
            for (PublisherCombineLatestInner<T> inner : subscribers) {
                inner.cancel();
            }
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            if ((requestedMode & THREAD_BARRIER) != 0) {
                return NONE;
            }
            int m = requestedMode & ASYNC;
            outputFused = m != 0;
            return m;
        }
        
        @Override
        public R poll() {
            SourceAndArray e = queue.poll();
            if (e == null) {
                return null;
            }
            R r = combiner.apply(e.array);
            e.source.requestOne();
            return r;
        }
        
        @Override
        public void clear() {
            queue.clear();
        }
        
        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }
        
        @Override
        public int size() {
            return queue.size();
        }
    }
    
    static final class PublisherCombineLatestInner<T>
            implements Subscriber<T>, Receiver, Producer,
                       SubscriberState {

        final PublisherCombineLatestCoordinator<T, ?> parent;

        final int index;

        final int prefetch;
        
        final int limit;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherCombineLatestInner, Subscription> S =
          AtomicReferenceFieldUpdater.newUpdater(PublisherCombineLatestInner.class, Subscription.class, "s");

        int produced;
        
        
        public PublisherCombineLatestInner(PublisherCombineLatestCoordinator<T, ?> parent, int index, int prefetch) {
            this.parent = parent;
            this.index = index;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(prefetch);
            }
        }

        @Override
        public void onNext(T t) {
            parent.innerValue(index, t);
        }

        @Override
        public void onError(Throwable t) {
            parent.innerError(t);
        }

        @Override
        public void onComplete() {
            parent.innerComplete(index);
        }
        
        public void cancel() {
            SubscriptionHelper.terminate(S, this);
        }
        
        public void requestOne() {
            
            int p = produced + 1;
            if (p == limit) {
                produced = 0;
                s.request(p);
            } else {
                produced = p;
            }
            
        }

        @Override
        public Object downstream() {
            return parent;
        }

        @Override
        public long requestedFromDownstream() {
            return produced;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public long limit() {
            return limit;
        }

        @Override
        public long expectedFromUpstream() {
            return limit - produced;
        }
    }
    
    /**
     * The queue element type for internal use with PublisherCombineLatest.
     */
    public static final class SourceAndArray {
        final PublisherCombineLatestInner<?> source;
        final Object[] array;
        
        SourceAndArray(PublisherCombineLatestInner<?> source, Object[] array) {
            this.source = source;
            this.array = array;
        }
    }
}
