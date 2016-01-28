package reactivestreams.commons.publisher;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscriber.DeferredScalarSubscriber;

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
        DeferredScalarSubscriber<T, T> sds = new DeferredScalarSubscriber<>(s);
        
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
