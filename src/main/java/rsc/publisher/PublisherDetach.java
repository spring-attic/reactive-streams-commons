package rsc.publisher;

import org.reactivestreams.*;

import rsc.util.SubscriptionHelper;

/**
 * Detaches the both the child Subscriber and the Subscription on
 * termination or cancellation.
 * <p>This should help with odd retention scenarios when running
 * wit non Rx mentality based Publishers.
 * 
 * @param <T> the value type
 */
public final class PublisherDetach<T> extends PublisherSource<T, T> {

    public PublisherDetach(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherDetachSubscriber<>(s));
    }
    
    static final class PublisherDetachSubscriber<T> implements Subscriber<T>, Subscription {
        
        Subscriber<? super T> actual;
        
        Subscription s;

        public PublisherDetachSubscriber(Subscriber<? super T> actual) {
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
            Subscriber<? super T> a = actual;
            if (a != null) {
                a.onNext(t);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            Subscriber<? super T> a = actual;
            if (a != null) {
                actual = null;
                s = null;
                
                a.onError(t);
            }
        }
        
        @Override
        public void onComplete() {
            Subscriber<? super T> a = actual;
            if (a != null) {
                actual = null;
                s = null;
                
                a.onComplete();
            }
        }
        
        @Override
        public void request(long n) {
            Subscription a = s;
            if (a != null) {
                a.request(n);
            }
        }
        
        @Override
        public void cancel() {
            Subscription a = s;
            if (a != null) {
                actual = null;
                s = null;
                
                a.cancel();
            }
        }
    }
}
