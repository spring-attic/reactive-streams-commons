package reactivestreams.commons.publisher;

import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import reactivestreams.commons.util.*;

/**
 * Intercepts the onSubscribe call and makes sure calls to Subscription methods
 * only happen after the child Subscriber has returned from its onSubscribe method.
 * 
 * <p>This helps with child Subscribers that don't expect a recursive call from
 * onSubscribe into their onNext because, for example, they request immediately from
 * their onSubscribe but don't finish their preparation before that and onNext
 * runs into a half-prepared state. This can happen with non Rx mentality based Subscribers.
 *
 * @param <T> the value type
 */
public final class PublisherAwaitOnSubscribe<T> extends PublisherSource<T, T> {

    public PublisherAwaitOnSubscribe(Publisher<? extends T> source) {
        super(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherPostOnSubscribeSubscriber<>(s));
    }
    
    static final class PublisherPostOnSubscribeSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherPostOnSubscribeSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherPostOnSubscribeSubscriber.class, Subscription.class, "s");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherPostOnSubscribeSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherPostOnSubscribeSubscriber.class, "requested");

        public PublisherPostOnSubscribeSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                
                actual.onSubscribe(this);
                
                if (SubscriptionHelper.setOnce(S, this, s)) {
                    long r = REQUESTED.getAndSet(this, 0L);
                    if (r != 0L) {
                        s.request(r);
                    }
                }
            }
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            Subscription a = s;
            if (a != null) {
                a.request(n);
            } else {
                if (SubscriptionHelper.validate(n)) {
                    BackpressureHelper.getAndAddCap(REQUESTED, this, n);
                    a = s;
                    if (a != null) {
                        long r = REQUESTED.getAndSet(this, 0L);
                        if (r != 0L) {
                            a.request(n);
                        }
                    }
                }
            }
        }
        
        @Override
        public void cancel() {
            SubscriptionHelper.terminate(S, this);
        }
    }
}
