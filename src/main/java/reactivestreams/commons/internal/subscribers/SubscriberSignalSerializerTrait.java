package reactivestreams.commons.internal.subscribers;

import org.reactivestreams.Subscriber;

/**
 * Interface with default methods to handle Subscriber signal
 * serialization.
 */
public interface SubscriberSignalSerializerTrait<T> {

    Subscriber<? super T> serGetSubscriber();
    
    Object serGuard();
    
    boolean serIsEmitting();
    
    void serSetEmitting(boolean emitting);

    boolean serIsMissed();
    
    void serSetMissed(boolean missed);
    
    boolean serIsCancelled();
    
    boolean serIsDone();
    
    void serSetDone(boolean done);
    
    Throwable serGetError();
    
    void serSetError(Throwable error);
    
    SerializedSubscriber.LinkedArrayNode<T> serGetHead();
    
    void serSetHead(SerializedSubscriber.LinkedArrayNode<T> node);
    
    SerializedSubscriber.LinkedArrayNode<T> serGetTail();

    void serSetTail(SerializedSubscriber.LinkedArrayNode<T> node);
    
    default void serOnNext(T t) {
        if (serIsCancelled() || serIsDone()) {
            return;
        }
        
        synchronized (serGuard()) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }
            
            if (serIsEmitting()) {
                serAdd(t);
                serSetMissed(true);
                return;
            }
            
            serSetEmitting(true);
        }
        
        Subscriber<? super T> actual = serGetSubscriber();
        
        actual.onNext(t);
        
        serDrainLoop(actual);
    }
    
    default void serOnError(Throwable e) {
        if (serIsCancelled() || serIsDone()) {
            return;
        }
        
        synchronized (serGuard()) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }

            serSetDone(true);
            serSetError(e);

            if (serIsEmitting()) {
                serSetMissed(true);
                return;
            }
        }
        
        serGetSubscriber().onError(e);
    }
    
    default void serOnComplete() {
        if (serIsCancelled() || serIsDone()) {
            return;
        }
        
        synchronized (this) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }

            serSetDone(true);
            
            if (serIsEmitting()) {
                serSetMissed(true);
                return;
            }
        }
        
        serGetSubscriber().onComplete();
    }
    
    default void serDrainLoop(Subscriber<? super T> actual) {
        for (;;) {
            
            if (serIsCancelled()) {
                return;
            }
            
            boolean d;
            Throwable e;
            SerializedSubscriber.LinkedArrayNode<T> n;
            
            synchronized (serGuard()) {
                if (serIsCancelled()) {
                    return;
                }

                if (!serIsMissed()) {
                    serSetEmitting(false);
                    return;
                }
                
                serSetMissed(false);
                
                d = serIsDone();
                e = serGetError();
                n = serGetHead();
                
                serSetHead(null);
                serSetTail(null);
            }
            
            while (n != null) {

                T[] arr = n.array;
                int c = n.count;
                
                for (int i = 0; i < c; i++) {
                    
                    if (serIsCancelled()) {
                        return;
                    }
                    
                    actual.onNext(arr[i]);
                }
                
                n = n.next;
            }
            
            if (serIsCancelled()) {
                return;
            }
            
            if (e != null) {
                actual.onError(e);
                return;
            } else
            if (d) {
                actual.onComplete();
                return;
            }
        }
    }
    
    default void serAdd(T value) {
        SerializedSubscriber.LinkedArrayNode<T> t = serGetTail();
        
        if (t == null) {
            t = new SerializedSubscriber.LinkedArrayNode<>(value);
            
            serSetHead(t);
            serSetTail(t);
        } else {
            if (t.count == SerializedSubscriber.LinkedArrayNode.DEFAULT_CAPACITY) {
                SerializedSubscriber.LinkedArrayNode<T> n = new SerializedSubscriber.LinkedArrayNode<>(value);
                
                t.next = n;
                serSetTail(n);
            } else {
                t.array[t.count++] = value;
            }
        }
    }
}
