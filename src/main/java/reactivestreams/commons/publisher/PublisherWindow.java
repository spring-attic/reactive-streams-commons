package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.error.*;
import reactivestreams.commons.support.*;

/**
 * Splits the source sequence into possibly overlapping publishers.
 * 
 * @param <T> the value type
 */
public final class PublisherWindow<T> extends PublisherSource<T, Publisher<T>> {

    final int size;
    
    final int skip;
    
    final Supplier<? extends Queue<T>> processorQueueSupplier;

    final Supplier<? extends Queue<Processor<T, T>>> overflowQueueSupplier;

    public PublisherWindow(Publisher<? extends T> source, int size, 
            Supplier<? extends Queue<T>> processorQueueSupplier) {
        super(source);
        if (size <= 0) {
            throw new IllegalArgumentException("size > 0 required but it was " + size);
        }
        this.size = size;
        this.skip = size;
        this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
        this.overflowQueueSupplier = null; // won't be needed here
    }

    
    public PublisherWindow(Publisher<? extends T> source, int size, int skip, 
            Supplier<? extends Queue<T>> processorQueueSupplier,
            Supplier<? extends Queue<Processor<T, T>>> overflowQueueSupplier) {
        super(source);
        if (size <= 0) {
            throw new IllegalArgumentException("size > 0 required but it was " + size);
        }
        if (skip <= 0) {
            throw new IllegalArgumentException("skip > 0 required but it was " + skip);
        }
        this.size = size;
        this.skip = skip;
        this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
        this.overflowQueueSupplier = Objects.requireNonNull(overflowQueueSupplier, "overflowQueueSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super Publisher<T>> s) {
        if (skip == size) {
            source.subscribe(new PublisherWindowExact<>(s, size, processorQueueSupplier));
        } else
        if (skip > size) {
            source.subscribe(new PublisherWindowSkip<>(s, size, skip, processorQueueSupplier));
        } else {
            Queue<Processor<T, T>> overflowQueue;
            
            try {
                overflowQueue = overflowQueueSupplier.get();
            } catch (Throwable e) {
                EmptySubscription.error(s, e);
                return;
            }
            
            if (overflowQueue == null) {
                EmptySubscription.error(s, new NullPointerException("The overflowQueueSupplier returned a null queue"));
                return;
            }
            
            source.subscribe(new PublisherWindowOverlap<>(s, size, skip, processorQueueSupplier, overflowQueue));
        }
    }
    
    static final class PublisherWindowExact<T> implements Subscriber<T>, Subscription, Runnable {
        
        final Subscriber<? super Publisher<T>> actual;

        final Supplier<? extends Queue<T>> processorQueueSupplier;
        
        final int size;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowExact> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowExact.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowExact> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowExact.class, "once");

        int index;
        
        Subscription s;
        
        Processor<T, T> window;
        
        boolean done;
        
        public PublisherWindowExact(Subscriber<? super Publisher<T>> actual, int size,
                Supplier<? extends Queue<T>> processorQueueSupplier) {
            this.actual = actual;
            this.size = size;
            this.processorQueueSupplier = processorQueueSupplier;
            this.wip = 1;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            int i = index;
            
            Processor<T, T> w = window;
            if (i == 0) {
                WIP.getAndIncrement(this);
                
                
                Queue<T> q;
                
                try {
                    q = processorQueueSupplier.get();
                } catch (Throwable ex) {
                    done = true;
                    cancel();
                    
                    actual.onError(ex);
                    return;
                }
                
                if (q == null) {
                    done = true;
                    cancel();
                    
                    actual.onError(new NullPointerException("The processorQueueSupplier returned a null queue"));
                    return;
                }
                
                w = new UnicastProcessor<>(q, this);
                window = w;
                
                actual.onNext(w);
            }
            
            i++;
            
            w.onNext(t);
            
            if (i == size) {
                index = 0;
                window = null;
                w.onComplete();
            } else {
                index = i;
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            Processor<T, T> w = window;
            if (w != null) {
                window = null;
                w.onError(t);
            }
            
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }

            Processor<T, T> w = window;
            if (w != null) {
                window = null;
                w.onComplete();
            }
            
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                long u = BackpressureHelper.multiplyCap(size, n);
                s.request(u);
            }
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                run();
            }
        }

        @Override
        public void run() {
            if (WIP.decrementAndGet(this) == 0) {
                s.cancel();
            }
        }
    }
    
    static final class PublisherWindowSkip<T> implements Subscriber<T>, Subscription, Runnable {
        
        final Subscriber<? super Publisher<T>> actual;

        final Supplier<? extends Queue<T>> processorQueueSupplier;
        
        final int size;
        
        final int skip;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowSkip> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowSkip.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowSkip> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowSkip.class, "once");

        volatile int firstRequest;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowSkip> FIRST_REQUEST =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowSkip.class, "firstRequest");

        int index;
        
        Subscription s;
        
        Processor<T, T> window;
        
        boolean done;
        
        public PublisherWindowSkip(Subscriber<? super Publisher<T>> actual, int size, int skip,
                Supplier<? extends Queue<T>> processorQueueSupplier) {
            this.actual = actual;
            this.size = size;
            this.skip = skip;
            this.processorQueueSupplier = processorQueueSupplier;
            this.wip = 1;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            int i = index;
            
            Processor<T, T> w = window;
            if (i == 0) {
                WIP.getAndIncrement(this);
                
                
                Queue<T> q;
                
                try {
                    q = processorQueueSupplier.get();
                } catch (Throwable ex) {
                    done = true;
                    cancel();
                    
                    actual.onError(ex);
                    return;
                }
                
                if (q == null) {
                    done = true;
                    cancel();
                    
                    actual.onError(new NullPointerException("The processorQueueSupplier returned a null queue"));
                    return;
                }
                
                w = new UnicastProcessor<>(q, this);
                window = w;
                
                actual.onNext(w);
            }
            
            i++;
            
            if (w != null) {
                w.onNext(t);
            }
            
            if (i == size) {
                window = null;
                w.onComplete();
            }
            
            if (i == skip) {
                index = 0;
            } else {
                index = i;
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            Processor<T, T> w = window;
            if (w != null) {
                window = null;
                w.onError(t);
            }
            
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }

            Processor<T, T> w = window;
            if (w != null) {
                window = null;
                w.onComplete();
            }
            
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
                    long u = BackpressureHelper.multiplyCap(size, n);
                    long v = BackpressureHelper.multiplyCap(skip - size, n - 1);
                    long w = BackpressureHelper.addCap(u, v);
                    s.request(w);
                } else {
                    long u = BackpressureHelper.multiplyCap(skip, n);
                    s.request(u);
                }
            }
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                run();
            }
        }

        @Override
        public void run() {
            if (WIP.decrementAndGet(this) == 0) {
                s.cancel();
            }
        }
    }

    static final class PublisherWindowOverlap<T> implements Subscriber<T>, Subscription, Runnable {
        
        final Subscriber<? super Publisher<T>> actual;

        final Supplier<? extends Queue<T>> processorQueueSupplier;

        final Queue<Processor<T, T>> queue;
        
        final int size;
        
        final int skip;

        final ArrayDeque<Processor<T, T>> windows;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowOverlap> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowOverlap.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowOverlap> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowOverlap.class, "once");

        volatile int firstRequest;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowOverlap> FIRST_REQUEST =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowOverlap.class, "firstRequest");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherWindowOverlap> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherWindowOverlap.class, "requested");

        volatile int dw;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherWindowOverlap> DW =
                AtomicIntegerFieldUpdater.newUpdater(PublisherWindowOverlap.class, "dw");

        int index;
        
        int produced;
        
        Subscription s;
        
        volatile boolean done;
        Throwable error;
        
        volatile boolean cancelled;
        
        public PublisherWindowOverlap(Subscriber<? super Publisher<T>> actual, int size, int skip,
                Supplier<? extends Queue<T>> processorQueueSupplier,
                Queue<Processor<T, T>> overflowQueue) {
            this.actual = actual;
            this.size = size;
            this.skip = skip;
            this.processorQueueSupplier = processorQueueSupplier;
            this.wip = 1;
            this.queue = overflowQueue;
            this.windows = new ArrayDeque<>();
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            int i = index;
            
            if (i == 0) {
                if (!cancelled) {
                    WIP.getAndIncrement(this);
                    
                    
                    Queue<T> q;
                    
                    try {
                        q = processorQueueSupplier.get();
                    } catch (Throwable ex) {
                        done = true;
                        cancel();
                        
                        actual.onError(ex);
                        return;
                    }
                    
                    if (q == null) {
                        done = true;
                        cancel();
                        
                        actual.onError(new NullPointerException("The processorQueueSupplier returned a null queue"));
                        return;
                    }
                    
                    Processor<T, T> w = new UnicastProcessor<>(q, this);
                    
                    windows.offer(w);
                    
                    queue.offer(w);
                    drain();
                }
            }
            
            i++;

            for (Processor<T, T> w : windows) {
                w.onNext(t);
            }
            
            int p = produced + 1;
            if (p == size) {
                produced = p - skip;
                
                Processor<T, T> w = windows.poll();
                if (w != null) {
                    w.onComplete();
                }
            } else {
                produced = p;
            }
            
            if (i == skip) {
                index = 0;
            } else {
                index = i;
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }

            for (Processor<T, T> w : windows) {
                w.onError(t);
            }
            windows.clear();
            
            error = t;
            done = true;
            drain();
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }

            for (Processor<T, T> w : windows) {
                w.onComplete();
            }
            windows.clear();
            
            done = true;
            drain();
        }
        
        void drain() {
            if (DW.getAndIncrement(this) != 0) {
                return;
            }
            
            final Subscriber<? super Publisher<T>> a = actual;
            final Queue<Processor<T, T>> q = queue;
            int missed = 1;
            
            for (;;) {
                
                long r = requested;
                long e = 0;
                
                while (e != r) {
                    boolean d = done;
                    
                    Processor<T, T> t = q.poll();
                    
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
                
                if (e == r) {
                    if (checkTerminated(done, q.isEmpty(), a, q)) {
                        return;
                    }
                }
                
                if (e != 0L && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }
                
                missed = DW.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                q.clear();
                return true;
            }
            
            if (d) {
                Throwable e = error;
                
                if (e != null) {
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
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
                    long u = BackpressureHelper.multiplyCap(skip, n - 1);
                    long v = BackpressureHelper.addCap(size, u);
                    s.request(v);
                } else {
                    long u = BackpressureHelper.multiplyCap(skip, n);
                    s.request(u);
                }
                
                BackpressureHelper.addAndGet(REQUESTED, this, n);
                drain();
            }
        }
        
        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                run();
            }
        }

        @Override
        public void run() {
            if (WIP.decrementAndGet(this) == 0) {
                s.cancel();
            }
        }
    }

}
