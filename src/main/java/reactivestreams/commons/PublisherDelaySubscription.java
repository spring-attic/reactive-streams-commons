package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SingleSubscriptionArbiter;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Delays the subscription to the main source until another Publisher
 * signals a value or completes.
 * 
 * @param <T> the main source value type
 * @param <U> the other source type
 */
public final class PublisherDelaySubscription<T, U> implements Publisher<T> {
    
    final Publisher<? extends T> source;
    
    final Publisher<U> other;

    public PublisherDelaySubscription(Publisher<? extends T> source, Publisher<U> other) {
        this.source = Objects.requireNonNull(source, "source");
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        other.subscribe(new PublisherDelaySubscriptionOtherSubscriber<>(s, source));
    }
    
    static final class PublisherDelaySubscriptionOtherSubscriber<T, U> 
    extends SingleSubscriptionArbiter<U, T> {

        final Publisher<? extends T> source;

        Subscription s;
        
        boolean done;
        
        public PublisherDelaySubscriptionOtherSubscriber(Subscriber<? super T> actual, Publisher<? extends T> source) {
            super(actual);
            this.source = source;
        }

        @Override
        public void cancel() {
            s.cancel();
            super.cancel();
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                subscriber.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(U t) {
            if (done) {
                return;
            }
            done = true;
            s.cancel();
            
            subscribeSource();
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            subscribeSource();
        }
        
        void subscribeSource() {
            source.subscribe(new PublisherDelaySubscriptionMainSubscriber<>(subscriber, this));
        }
        
        static final class PublisherDelaySubscriptionMainSubscriber<T> implements Subscriber<T> {
            
            final Subscriber<? super T> actual;
            
            final SingleSubscriptionArbiter<?, ?> arbiter;

            public PublisherDelaySubscriptionMainSubscriber(Subscriber<? super T> actual,
                    SingleSubscriptionArbiter<?, ?> arbiter) {
                this.actual = actual;
                this.arbiter = arbiter;
            }

            @Override
            public void onSubscribe(Subscription s) {
                arbiter.set(s);
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
            
            
        }
    }
}
