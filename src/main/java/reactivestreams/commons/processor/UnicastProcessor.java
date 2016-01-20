package reactivestreams.commons.processor;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.publisher.PublisherBase;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * A Processor implementation that takes a custom queue and allows
 * only a single subscriber.
 * 
 * <p>
 * The implementation keeps the order of signals.
 *
 * @param <T> the input and output type
 */
public final class UnicastProcessor<T> 
extends PublisherBase<T>
implements Processor<T, T> {

    final Queue<T> queue;
    
    volatile Runnable onTerminate;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<UnicastProcessor, Runnable> ON_TERMINATE =
            AtomicReferenceFieldUpdater.newUpdater(UnicastProcessor.class, Runnable.class, "onTerminate");
    
    volatile boolean done;
    Throwable error;
    
    volatile Subscriber<? super T> actual;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<UnicastProcessor, Subscriber> ACTUAL =
            AtomicReferenceFieldUpdater.newUpdater(UnicastProcessor.class, Subscriber.class, "actual");
    
    volatile boolean cancelled;
    
    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<UnicastProcessor> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "once");

    volatile int wip;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<UnicastProcessor> WIP =
            AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "wip");

    volatile long requested;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<UnicastProcessor> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(UnicastProcessor.class, "requested");
    
    public UnicastProcessor(Queue<T> queue) {
        this(queue, null);
    }
    
    public UnicastProcessor(Queue<T> queue, Runnable onTerminate) {
        this.queue = queue;
        this.onTerminate = onTerminate;
    }
    
    void doTerminate() {
        Runnable r = onTerminate;
        if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
            r.run();
        }
    }
    
    void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }
        
        int missed = 1;
        
        final Queue<T> q = queue;
        Subscriber<? super T> a = actual;
        
        
        for (;;) {

            if (a != null) {
                long r = requested;
                long e = 0L;
                
                while (r != e) {
                    boolean d = done;
                    
                    T t = q.poll();
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
                
                if (e != 0 && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
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
    
    boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a, Queue<T> q) {
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
    
    @Override
    public void onSubscribe(Subscription s) {
        if (done || cancelled) {
            s.cancel();
        } else {
            s.request(Long.MAX_VALUE);
        }
    }
    
    @Override
    public void onNext(T t) {
        if (done || cancelled) {
            return;
        }
        
        if (!queue.offer(t)) {
            error = new IllegalStateException("The queue is full");
            done = true;
        }
        drain();
    }
    
    @Override
    public void onError(Throwable t) {
        if (done || cancelled) {
            return;
        }
        
        error = t;
        done = true;

        doTerminate();
        
        drain();
    }
    
    @Override
    public void onComplete() {
        if (done || cancelled) {
            return;
        }
        
        done = true;

        doTerminate();
        
        drain();
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
            
            s.onSubscribe(new UnicastSubscription());
            actual = s;
            if (cancelled) {
                actual = null;
            } else {
                drain();
            }
        } else {
            s.onError(new IllegalStateException("This processor allows only a single Subscriber"));
        }
    }
    
    void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            BackpressureHelper.addAndGet(REQUESTED, this, n);
            drain();
        }
    }
    
    void cancel() {
        if (cancelled) {
            return;
        }
        cancelled = true;

        doTerminate();

        if (WIP.getAndIncrement(this) == 0) {
            queue.clear();
        }
    }
    
    final class UnicastSubscription implements Subscription {
        @Override
        public void request(long n) {
            UnicastProcessor.this.request(n);
        }
        
        @Override
        public void cancel() {
            UnicastProcessor.this.cancel();
        }
    }
}
