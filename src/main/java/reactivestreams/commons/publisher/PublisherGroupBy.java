package reactivestreams.commons.publisher;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.MultiProducer;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.state.Backpressurable;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.state.Failurable;
import reactivestreams.commons.state.Requestable;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Groups upstream items into their own Publisher sequence based on a key selector.
 *
 * @param <T> the source value type
 * @param <K> the key value type
 * @param <V> the group item value type
 */
public final class PublisherGroupBy<T, K, V> extends PublisherSource<T, GroupedPublisher<K, V>>
implements Fuseable, Backpressurable  {

    final Function<? super T, ? extends K> keySelector;
    
    final Function<? super T, ? extends V> valueSelector;
    
    final Supplier<? extends Queue<V>> groupQueueSupplier;

    final Supplier<? extends Queue<GroupedPublisher<K, V>>> mainQueueSupplier;

    final int prefetch;

    public PublisherGroupBy(
            Publisher<? extends T> source, 
            Function<? super T, ? extends K> keySelector,
            Function<? super T, ? extends V> valueSelector,
            Supplier<? extends Queue<GroupedPublisher<K, V>>> mainQueueSupplier, 
            Supplier<? extends Queue<V>> groupQueueSupplier, 
            int prefetch) {
        super(source);
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        this.keySelector = Objects.requireNonNull(keySelector, "keySelector");
        this.valueSelector = Objects.requireNonNull(valueSelector, "valueSelector");
        this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
        this.groupQueueSupplier = Objects.requireNonNull(groupQueueSupplier, "groupQueueSupplier");
        this.prefetch = prefetch;
    }
    
    @Override
    public void subscribe(Subscriber<? super GroupedPublisher<K, V>> s) {
        Queue<GroupedPublisher<K, V>> q;
        
        try {
            q = mainQueueSupplier.get();
        } catch (Throwable ex) {
            ExceptionHelper.throwIfFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The mainQueueSupplier returned a null queue"));
            return;
        }
        
        source.subscribe(new PublisherGroupByMain<>(s, q, groupQueueSupplier, prefetch, keySelector, valueSelector));
    }
    
    static final class PublisherGroupByMain<T, K, V> implements Subscriber<T>, 
    Fuseable.QueueSubscription<GroupedPublisher<K, V>>, MultiProducer, Backpressurable, Producer, Requestable,
                                                                Failurable, Cancellable, Completable, Receiver {

        final Function<? super T, ? extends K> keySelector;
        
        final Function<? super T, ? extends V> valueSelector;
        
        final Subscriber<? super GroupedPublisher<K, V>> actual;

        final Queue<GroupedPublisher<K, V>> queue;
        
        final Supplier<? extends Queue<V>> groupQueueSupplier;

        final int prefetch;
        
        final ConcurrentMap<K, UnicastGroupedPublisher<K, V>> groupMap; 
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherGroupByMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherGroupByMain.class, "wip");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherGroupByMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherGroupByMain.class, "requested");
        
        volatile boolean done;
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherGroupByMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherGroupByMain.class, Throwable.class, "error");
        
        volatile int cancelled;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherGroupByMain> CANCELLED =
                AtomicIntegerFieldUpdater.newUpdater(PublisherGroupByMain.class, "cancelled");
        
        volatile int groupCount;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherGroupByMain> GROUP_COUNT =
                AtomicIntegerFieldUpdater.newUpdater(PublisherGroupByMain.class, "groupCount");

        Subscription s;
        
        volatile boolean enableAsyncFusion;
        
        public PublisherGroupByMain(
                Subscriber<? super GroupedPublisher<K, V>> actual,
                Queue<GroupedPublisher<K, V>> queue, 
                Supplier<? extends Queue<V>> groupQueueSupplier, 
                int prefetch,
                Function<? super T, ? extends K> keySelector,
                Function<? super T, ? extends V> valueSelector
                ) {
            this.actual = actual;
            this.queue = queue;
            this.groupQueueSupplier = groupQueueSupplier;
            this.prefetch = prefetch;
            this.groupMap = new ConcurrentHashMap<>();
            this.keySelector = keySelector;
            this.valueSelector = valueSelector;
            GROUP_COUNT.lazySet(this, 1);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
                s.request(prefetch);
            }
        }
        
        @Override
        public void onNext(T t) {
            K key;
            V value;
            
            try {
                key = keySelector.apply(t);
                value = valueSelector.apply(t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                s.cancel();
                
                ExceptionHelper.addThrowable(ERROR, this, ex);
                done = true;
                if (enableAsyncFusion) {
                    signalAsyncError();
                } else {
                    drain();
                }
                return;
            }
            if (key == null) {
                NullPointerException ex = new NullPointerException("The keySelector returned a null value");
                s.cancel();
                ExceptionHelper.addThrowable(ERROR, this, ex);
                done = true;
                if (enableAsyncFusion) {
                    signalAsyncError();
                } else {
                    drain();
                }
                return;
            }
            if (value == null) {
                NullPointerException ex = new NullPointerException("The valueSelector returned a null value");
                s.cancel();
                ExceptionHelper.addThrowable(ERROR, this, ex);
                done = true;
                if (enableAsyncFusion) {
                    signalAsyncError();
                } else {
                    drain();
                }
                return;
            }
            
            UnicastGroupedPublisher<K, V> g = groupMap.get(key);
            
            if (g == null) {
                // if the main is cancelled, don't create new groups
                if (cancelled == 0) {
                    Queue<V> q;
                    
                    try {
                        q = groupQueueSupplier.get();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        s.cancel();
                        
                        ExceptionHelper.addThrowable(ERROR, this, ex);
                        done = true;
                        if (enableAsyncFusion) {
                            signalAsyncError();
                        } else {
                            drain();
                        }
                        return;
                    }
                    
                    GROUP_COUNT.getAndIncrement(this);
                    g = new UnicastGroupedPublisher<>(key, q, this);
                    g.onNext(value);
                    groupMap.put(key, g);
                    
                    queue.offer(g);
                    if (enableAsyncFusion) {
                        actual.onNext(null);
                    } else {
                        drain();
                    }
                }
            } else {
                g.onNext(value);
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
            if (enableAsyncFusion) {
                signalAsyncComplete();
            } else {
                drain();
            }
        }

        @Override
        public long getCapacity() {
            return prefetch;
        }

        @Override
        public long getPending() {
            return queue.size();
        }

        @Override
        public boolean isStarted() {
            return s != null && cancelled != 1 && !done;
        }

        @Override
        public boolean isCancelled() {
            return cancelled == 1;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Iterator<?> downstreams() {
            return groupMap.values().iterator();
        }

        @Override
        public long downstreamCount() {
            return GROUP_COUNT.get(this);
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        void signalAsyncComplete() {
            groupCount = 0;
            for (UnicastGroupedPublisher<K, V> g : groupMap.values()) {
                g.onComplete();
            }
            actual.onComplete();
            groupMap.clear();
        }

        void signalAsyncError() {
            Throwable e = ExceptionHelper.terminate(ERROR, this);
            groupCount = 0;
            for (UnicastGroupedPublisher<K, V> g : groupMap.values()) {
                g.onError(e);
            }
            actual.onError(e);
            groupMap.clear();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (enableAsyncFusion) {
                    actual.onNext(null);
                } else {
                    BackpressureHelper.addAndGet(REQUESTED, this, n);
                    drain();
                }
            }
        }
        
        @Override
        public void cancel() {
            if (CANCELLED.compareAndSet(this, 0, 1)) {
                if (GROUP_COUNT.decrementAndGet(this) == 0) {
                    s.cancel();
                } else {
                    if (!enableAsyncFusion) {
                        if (WIP.getAndIncrement(this) == 0) {
                            // remove queued up but unobservable groups from the mapping
                            GroupedPublisher<K, V> g;
                            while ((g = queue.poll()) != null) {
                                ((UnicastGroupedPublisher<K, V>)g).cancel();
                            }
                            
                            if (WIP.decrementAndGet(this) == 0) {
                                return;
                            }
                            
                            drainLoop();
                        }
                    }
                }
            }
        }
        
        void groupTerminated(K key) {
            if (groupCount == 0) {
                return;
            }
            groupMap.remove(key);
            if (GROUP_COUNT.decrementAndGet(this) == 0) {
                s.cancel();
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            drainLoop();
        }
        void drainLoop() {
            
            int missed = 1;
            
            Subscriber<? super GroupedPublisher<K, V>> a = actual;
            Queue<GroupedPublisher<K, V>> q = queue;
            
            for (;;) {
                
                long r = requested;
                long e = 0L;
                
                while (e != r) {
                    boolean d = done;
                    GroupedPublisher<K, V> v = q.poll();
                    boolean empty = v == null;
                    
                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    a.onNext(v);
                    
                    e++;
                }
                
                if (e == r) {
                    if (checkTerminated(done, q.isEmpty(), a, q)) {
                        return;
                    }
                }
                
                if (e != 0L) {
                    
                    s.request(e);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.addAndGet(this, -e);
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<GroupedPublisher<K, V>> q) {
            if (d) {
                Throwable e = error;
                if (e != null && e != ExceptionHelper.TERMINATED) {
                    queue.clear();
                    signalAsyncError();
                    return true;
                } else
                if (empty) {
                    signalAsyncComplete();
                    return true;
                }
            }
            
            return false;
        }

        @Override
        public GroupedPublisher<K, V> poll() {
            return queue.poll();
        }

        @Override
        public GroupedPublisher<K, V> peek() {
            return queue.peek();
        }

        @Override
        public boolean add(GroupedPublisher<K, V> t) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean offer(GroupedPublisher<K, V> t) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public GroupedPublisher<K, V> remove() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public GroupedPublisher<K, V> element() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public Iterator<GroupedPublisher<K, V>> iterator() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean addAll(Collection<? extends GroupedPublisher<K, V>> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            queue.clear();
        }

        @Override
        public int requestFusion(int requestedMode) {
            if (requestedMode == Fuseable.ANY || requestedMode == Fuseable.ASYNC) {
                enableAsyncFusion = true;
                return Fuseable.ASYNC;
            }
            return Fuseable.NONE;
        }
        
        @Override
        public void drop() {
            queue.poll();
        }
    }
    
    static final class UnicastGroupedPublisher<K, V> extends GroupedPublisher<K, V> 
    implements Fuseable, Fuseable.QueueSubscription<V>,
    Producer, Receiver, Failurable, Completable, Cancellable, Requestable, Backpressurable {
        final K key;

        @Override
        public K key() {
            return key;
        }
        
        final Queue<V> queue;
        
        volatile PublisherGroupByMain<?, K, V> parent;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<UnicastGroupedPublisher, PublisherGroupByMain> PARENT =
                AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedPublisher.class, PublisherGroupByMain.class, "parent");
        
        volatile boolean done;
        Throwable error;
        
        volatile Subscriber<? super V> actual;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<UnicastGroupedPublisher, Subscriber> ACTUAL =
                AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedPublisher.class, Subscriber.class, "actual");
        
        volatile boolean cancelled;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<UnicastGroupedPublisher> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedPublisher.class, "once");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<UnicastGroupedPublisher> WIP =
                AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedPublisher.class, "wip");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<UnicastGroupedPublisher> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(UnicastGroupedPublisher.class, "requested");
        
        volatile boolean enableOperatorFusion;

        public UnicastGroupedPublisher(K key, Queue<V> queue, PublisherGroupByMain<?, K, V> parent) {
            this.key = key;
            this.queue = queue;
            this.parent = parent;
        }
        
        void doTerminate() {
            PublisherGroupByMain<?, K, V> r = parent;
            if (r != null && PARENT.compareAndSet(this, r, null)) {
                r.groupTerminated(key);
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            
            final Queue<V> q = queue;
            Subscriber<? super V> a = actual;
            
            
            for (;;) {

                if (a != null) {
                    long r = requested;
                    long e = 0L;
                    
                    while (r != e) {
                        boolean d = done;
                        
                        V t = q.poll();
                        boolean empty = t == null;
                        
                        if (checkTerminated(d, empty, a, q)) {
                            return;
                        }
                        
                        if (empty) {
                            break;
                        }
                        
                        a.onNext(t);
                        
                        e++;
                    }
                    
                    if (r == e) {
                        if (checkTerminated(done, q.isEmpty(), a, q)) {
                            return;
                        }
                    }
                    
                    if (e != 0) {
                        parent.request(e);
                        if (r != Long.MAX_VALUE) {
                            REQUESTED.addAndGet(this, -e);
                        }
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
                
                if (a == null) {
                    a = actual;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                q.clear();
                actual = null;
                return true;
            }
            if (d && empty) {
                Throwable e = error;
                actual = null;
                if (e != null) {
                    a.onError(e);
                } else {
                    a.onComplete();
                }
                return true;
            }
            
            return false;
        }
        
        public void onNext(V t) {
            if (done || cancelled) {
                return;
            }
            
            if (!queue.offer(t)) {
                error = new IllegalStateException("The queue is full");
                done = true;
            }
            if (enableOperatorFusion) {
                Subscriber<? super V> a = actual;
                if (a != null) {
                    a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
                }
            } else {
                drain();
            }
        }
        
        public void onError(Throwable t) {
            if (done || cancelled) {
                return;
            }
            
            error = t;
            done = true;

            doTerminate();
            
            if (enableOperatorFusion) {
                Subscriber<? super V> a = actual;
                if (a != null) {
                    a.onError(t);
                }
            } else {
                drain();
            }
        }
        
        public void onComplete() {
            if (done || cancelled) {
                return;
            }
            
            done = true;

            doTerminate();
            
            if (enableOperatorFusion) {
                Subscriber<? super V> a = actual;
                if (a != null) {
                    a.onComplete();
                }
            } else {
                drain();
            }
        }
        
        @Override
        public void subscribe(Subscriber<? super V> s) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                
                s.onSubscribe(this);
                actual = s;
                if (cancelled) {
                    actual = null;
                } else {
                    if (enableOperatorFusion) {
                        if (done) {
                            Throwable e = error;
                            if (e != null) {
                                s.onError(e);
                            } else {
                                s.onComplete();
                            }
                        } else {
                            s.onNext(null);
                        }
                    } else {
                        drain();
                    }
                }
            } else {
                s.onError(new IllegalStateException("This processor allows only a single Subscriber"));
            }
        }

        @Override
        public int getMode() {
            return INNER;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (enableOperatorFusion) {
                    Subscriber<? super V> a = actual;
                    if (a != null) {
                        a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
                    }
                } else {
                    BackpressureHelper.addAndGet(REQUESTED, this, n);
                    drain();
                }
            }
        }
        
        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;

            doTerminate();

            if (!enableOperatorFusion) {
                if (WIP.getAndIncrement(this) == 0) {
                    queue.clear();
                }
            }
        }
        
        @Override
        public V poll() {
            return queue.poll();
        }

        @Override
        public V peek() {
            return queue.peek();
        }

        @Override
        public boolean add(V t) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean offer(V t) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public V remove() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public V element() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public Iterator<V> iterator() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Operators should not use this method!");
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            queue.clear();
        }

        @Override
        public int requestFusion(int requestedMode) {
            if (requestedMode == Fuseable.ANY || requestedMode == Fuseable.ASYNC) {
                enableOperatorFusion = true;
                return Fuseable.ASYNC;
            }
            return Fuseable.NONE;
        }
        
        @Override
        public void drop() {
            queue.poll();
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }



        @Override
        public boolean isStarted() {
            return once == 1 && !done && !cancelled;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object upstream() {
            return parent;
        }

        @Override
        public long getCapacity() {
            return parent.prefetch;
        }

        @Override
        public long getPending() {
            return queue == null || done ? -1L : queue.size();
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

    }
}
