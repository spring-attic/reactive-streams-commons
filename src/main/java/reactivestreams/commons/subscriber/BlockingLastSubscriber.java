package reactivestreams.commons.subscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;

public class BlockingLastSubscriber<T> implements Subscriber<T> {
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    T value;
    
    Throwable error;
    
    Subscription s;
    
    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.validate(this.s, s)) {
            this.s = s;
            s.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(T t) {
        value = t;
    }

    @Override
    public void onError(Throwable t) {
        value = null;
        error = t;
        latch.countDown();
    }

    @Override
    public void onComplete() {
        latch.countDown();
    }

    public T blockingGet() {
        if (latch.getCount() != 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                ExceptionHelper.propagate(ex);
            }
        }
        Throwable e = error;
        if (e != null) {
            ExceptionHelper.propagate(e);
        }
        return value;
    }

    public T blockingGet(long timeout, TimeUnit unit) {
        if (latch.getCount() != 0) {
            try {
                if (!latch.await(timeout, unit)) {
                    ExceptionHelper.propagate(new TimeoutException());
                }
            } catch (InterruptedException ex) {
                ExceptionHelper.propagate(ex);
            }
        }
        
        Throwable e = error;
        if (e != null) {
            ExceptionHelper.propagate(e);
        }
        return value;
    }
}
