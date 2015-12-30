package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.support.SubscriptionHelper;

/**
 * Relays values until a predicate returns 
 * true, indicating the sequence should stop 
 * (checked after each value has been delivered).
 *
 * @param <T> the value type
 */
public final class PublisherTakeUntilPredicate<T> extends PublisherSource<T, T> {

    final Predicate<? super T> predicate;

    public PublisherTakeUntilPredicate(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherTakeUntilPredicateSubscriber<>(s, predicate));
    }
    
    static final class PublisherTakeUntilPredicateSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final Predicate<? super T> predicate;

        Subscription s;
        
        boolean done;
        
        public PublisherTakeUntilPredicateSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(s);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }

            actual.onNext(t);

            boolean b;
            
            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                
                onError(e);
                
                return;
            }
            
            if (b) {
                s.cancel();
                
                onComplete();
                
                return;
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            actual.onComplete();
        }
        
        
    }
}
