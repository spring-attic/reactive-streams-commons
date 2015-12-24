package reactivestreams.commons;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.*;

import reactivestreams.commons.internal.*;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */
public final class PublisherConcatArray<T> implements Publisher<T> {
    
    final Publisher<? extends T>[] array;
    
    @SafeVarargs
    public PublisherConcatArray(Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;
        
        if (a.length == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (a.length == 1) {
            Publisher<? extends T> p = a[0];
            
            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
            } else {
                p.subscribe(s);
            }
            return;
        }
    
        PublisherConcatArraySubscriber<T> parent = new PublisherConcatArraySubscriber<>(s, a);
    
        s.onSubscribe(parent.arbiter);
        
        if (!parent.arbiter.isCancelled()) {
            parent.onComplete();
        }
    }
    
    static final class PublisherConcatArraySubscriber<T> 
    extends AtomicInteger
    implements Subscriber<T> {
        /**
         * 
         */
        private static final long serialVersionUID = 3990198597327692577L;

        final Subscriber<? super T> actual;
        
        final Publisher<? extends T>[] sources;
        
        final MultiSubscriptionArbiter arbiter;
        
        int index;
        
        public PublisherConcatArraySubscriber(Subscriber<? super T> actual, Publisher<? extends T>[] sources) {
            this.actual = actual;
            this.sources = sources;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
            
            arbiter.producedOne();
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (getAndIncrement() == 0) {
                Publisher<? extends T>[] a = sources;
                do {
                    
                    if (arbiter.isCancelled()) {
                        return;
                    }

                    int i = index;
                    if (i == a.length) {
                        actual.onComplete();
                        return;
                    }
                    
                    Publisher<? extends T> p = a[i];
                    
                    if (p == null) {
                        actual.onError(new NullPointerException("The " + i  + "th source Publisher is null"));
                        return;
                    }
                    
                    p.subscribe(this);

                    if (arbiter.isCancelled()) {
                        return;
                    }

                    index = ++i;
                } while (decrementAndGet() != 0);
            }
            
        }
    }
}
