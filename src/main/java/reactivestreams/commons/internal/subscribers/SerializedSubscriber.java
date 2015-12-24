package reactivestreams.commons.internal.subscribers;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Subscriber that makes sure signals are delivered sequentially in case
 * the onNext, onError or onComplete methods are called concurrently.
 *
 * <p>
 * The implementation uses {@code synchronized (this)} to ensure mutual exclusion.
 * 
 * <p>
 * Note that the class implements Subscription to save on allocation.
 * 
 * @param <T> the value type
 */
public final class SerializedSubscriber<T> implements Subscriber<T>, Subscription {

    final Subscriber<? super T> actual;
    
    boolean emitting;
    
    boolean missed;
    
    
    volatile boolean done;
    
    volatile boolean cancelled;
    
    ArrayNode<T> head;
    
    ArrayNode<T> tail;
    
    Throwable error;

    Subscription s;
    
    public SerializedSubscriber(Subscriber<? super T> actual) {
        this.actual = actual;
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
        if (cancelled || done) {
            return;
        }
        
        synchronized (this) {
            if (cancelled || done) {
                return;
            }
            
            if (emitting) {
                add(t);
                missed = true;
                return;
            }
            
            emitting = true;
        }
        
        actual.onNext(t);
        
        drain();
    }
    
    void add(T value) {
        ArrayNode<T> t = tail;
        
        if (t == null) {
            t = new ArrayNode<>(value);
            
            head = t;
            tail = t;
        } else {
            if (t.count == ArrayNode.DEFAULT_CAPACITY) {
                ArrayNode<T> n = new ArrayNode<>(value);
                
                t.next = n;
                t = n;
            } else {
                t.array[t.count++] = value;
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        if (cancelled || done) {
            return;
        }
        
        synchronized (this) {
            if (cancelled || done) {
                return;
            }

            done = true;
            error = t;

            if (emitting) {
                missed = true;
                return;
            }
        }
        
        actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (cancelled || done) {
            return;
        }
        
        synchronized (this) {
            if (cancelled || done) {
                return;
            }

            done = true;
            if (emitting) {
                missed = true;
                return;
            }
        }
        
        actual.onComplete();
    }

    void drain() {
        
        Subscriber<? super T> a = actual;
        
        for (;;) {
            
            if (cancelled) {
                return;
            }
            
            boolean d;
            Throwable e;
            ArrayNode<T> n;
            
            synchronized (this) {
                if (cancelled) {
                    return;
                }

                if (!missed) {
                    emitting = false;
                    return;
                }
                
                missed = false;
                
                d = done;
                e = error;
                n = head;
                
                head = null;
                tail = null;
            }
            
            while (n != null) {

                T[] arr = n.array;
                int c = n.count;
                
                for (int i = 0; i < c; i++) {
                    
                    if (cancelled) {
                        return;
                    }
                    
                    a.onNext(arr[i]);
                }
                
                n = n.next;
            }
            
            if (cancelled) {
                return;
            }
            
            if (e != null) {
                a.onError(e);
                return;
            } else
            if (d) {
                a.onComplete();
                return;
            }
        }
    }
    
    @Override
    public void request(long n) {
        s.request(n);
    }

    @Override
    public void cancel() {
        cancelled = true;
        s.cancel();
    }
    
    static final class ArrayNode<T> {
        
        static final int DEFAULT_CAPACITY = 16;
        
        final T[] array;
        int count;
        ArrayNode<T> next;
        
        @SuppressWarnings("unchecked")
        public ArrayNode(T value) {
            array = (T[])new Object[DEFAULT_CAPACITY];
            array[0] = value;
            count = 1;
        }
    }
}
