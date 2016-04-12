package rsc.subscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

public class BlockingFirstSubscriber<T> implements Subscriber<T> {
    
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
        if (value == null) {
            value = t;
            s.cancel();
            latch.countDown();
        } else {
            UnsignalledExceptions.onNextDropped(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (value == null) {
            error = t;
            latch.countDown();
        } else {
            UnsignalledExceptions.onErrorDropped(t);
        }
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
