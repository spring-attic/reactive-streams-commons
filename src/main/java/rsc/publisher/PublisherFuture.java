package rsc.publisher;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Subscriber;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;

@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherFuture<T> extends Px<T> implements Fuseable {
    
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
