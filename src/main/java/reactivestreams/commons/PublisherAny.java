package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscriber.SubscriberDelayedScalar;
import reactivestreams.commons.internal.support.SubscriptionHelper;

/**
 * Emits a single boolean true if any of the values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with true if
 * the predicate matches a value.
 * 
 * @param <T> the source value type
 */
public final class PublisherAny<T> implements Publisher<Boolean> {

    final Publisher<? extends T> source;
    
    final Predicate<? super T> predicate;

    public PublisherAny(Publisher<? extends T> source, Predicate<? super T> predicate) {
        this.source = Objects.requireNonNull(source, "source");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }
    
    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherAnySubscriber<T>(s, predicate));
    }
    
    static final class PublisherAnySubscriber<T> extends SubscriberDelayedScalar<T, Boolean> {
        final Predicate<? super T> predicate;

        Subscription s;
        
        boolean done;
        
        public PublisherAnySubscriber(Subscriber<? super Boolean> actual, Predicate<? super T> predicate) {
            super(actual);
            this.predicate = predicate;
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
        public void onNext(T t) {

            if (done) {
                return;
            }
            
            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                done = true;
                s.cancel();
                
                subscriber.onError(e);
                return;
            }
            if (b) {
                done = true;
                s.cancel();
                
                set(true);
            }
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
            set(false);
        }
    }
}
