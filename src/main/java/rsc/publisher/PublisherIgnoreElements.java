package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.flow.Producer;
import rsc.flow.Receiver;
import rsc.subscriber.SubscriptionHelper;

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 */
public final class PublisherIgnoreElements<T> extends PublisherSource<T, T> {

    public PublisherIgnoreElements(Publisher<? extends T> source) {
        super(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherIgnoreElementsSubscriber<>(s));
    }
    
    static final class PublisherIgnoreElementsSubscriber<T> implements Subscriber<T>, Producer, Subscription,
                                                                       Receiver {
        final Subscriber<? super T> actual;
        
        Subscription s;
        
        public PublisherIgnoreElementsSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            // deliberately ignored
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
        public Object downstream() {
            return actual;
        }
        
        @Override
        public void request(long n) {
            // requests Long.MAX_VALUE anyway
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
        
        @Override
        public Object upstream() {
            return s;
        }
    }
}
