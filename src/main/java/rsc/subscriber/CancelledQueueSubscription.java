package rsc.subscriber;

import rsc.flow.Fuseable;
import rsc.flow.Fuseable.QueueSubscription;

public enum CancelledQueueSubscription implements QueueSubscription<Object> {
    INSTANCE;
    
    @SuppressWarnings("unchecked")
    public static <T> QueueSubscription<T> instance() {
        return (QueueSubscription<T>)INSTANCE;
    }
    
    @Override
    public void cancel() {
        
    }
    @Override
    public void clear() {
        
    }
    
    @Override
    public boolean isEmpty() {
        return true;
    }
    
    @Override
    public Object poll() {
        return null;
    }

    @Override
    public void request(long n) {
        
    }

    @Override
    public int requestFusion(int requestedMode) {
        return Fuseable.NONE;
    }
    
    @Override
    public int size() {
        return 0;
    }
}
