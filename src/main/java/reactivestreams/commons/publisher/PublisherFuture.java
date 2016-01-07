package reactivestreams.commons.publisher;

import java.util.concurrent.*;

import org.reactivestreams.Subscriber;

import reactivestreams.commons.subscriber.SubscriberDeferredScalar;

public final class PublisherFuture<T> extends PublisherBase<T> {
    
    final Future<? extends T> future;
    
    final long timeout;
    
    final TimeUnit unit;

    public PublisherFuture(Future<? extends T> future) {
        this.future = future;
        this.timeout = 0L;
        this.unit = null;
    }

    public PublisherFuture(Future<? extends T> future, long timeout, TimeUnit unit) {
        this.future = future;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        SubscriberDeferredScalar<T, T> sds = new SubscriberDeferredScalar<>(s);
        
        s.onSubscribe(sds);
        
        T v;
        try {
            if (unit != null) {
                v = future.get(timeout, unit);
            } else {
                v = future.get();
            }
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            s.onError(ex);
            return;
        }
        
        sds.complete(v);
    }
    
}
