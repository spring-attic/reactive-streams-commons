package reactivestreams.commons.processor;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.publisher.PublisherBase;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.state.Requestable;
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
implements Processor<T, T>, Fuseable.QueueSubscription<T>, Fuseable,
           Producer, Receiver, Completable, Cancellable, Requestable {

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
    
    volatile boolean enableOperatorFusion;

    public UnicastProcessor(Queue<T> queue) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.onTerminate = null;
    }

    public UnicastProcessor(Queue<T> queue, Runnable onTerminate) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
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
        
        Subscriber<? super T> a = actual;
        if (!queue.offer(t)) {
            IllegalStateException ex = new IllegalStateException("The queue is full");
            error = ex;
            done = true;
            
            doTerminate();
            if (enableOperatorFusion) {
                a.onError(ex);
            } else {
                drain();
            }
            return;
        }
        if (enableOperatorFusion) {
            if (a != null) {
                a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
            }
        } else {
            drain();
        }
    }
    
    @Override
    public void onError(Throwable t) {
        if (done || cancelled) {
            return;
        }
        
        error = t;
        done = true;

        doTerminate();
        
        if (enableOperatorFusion) {
            Subscriber<? super T> a = actual;
            if (a != null) {
                a.onError(t);
            }
        } else {
            drain();
        }
    }
    
    @Override
    public void onComplete() {
        if (done || cancelled) {
            return;
        }
        
        done = true;

        doTerminate();
        
        if (enableOperatorFusion) {
            Subscriber<? super T> a = actual;
            if (a != null) {
                a.onComplete();
            }
        } else {
            drain();
        }
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
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
                Subscriber<? super T> a = actual;
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
    public T poll() {
        return queue.poll();
    }

    @Override
    public T peek() {
        return queue.peek();
    }

    @Override
    public int size() {
        return queue.size();
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
        if ((requestedMode & Fuseable.ASYNC) != 0) {
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
        return onTerminate;
    }

    @Override
    public long requestedFromDownstream() {
        return requested;
    }
}