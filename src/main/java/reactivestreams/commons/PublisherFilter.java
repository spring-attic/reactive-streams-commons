package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.*;

import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */
public final class PublisherFilter<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Predicate<? super T> predicate;
    
    public PublisherFilter(Publisher<? extends T> source, Predicate<? super T> predicate) {
        this.source = Objects.requireNonNull(source);
        this.predicate = Objects.requireNonNull(predicate);
    }
    
    public Publisher<? extends T> source() {
        return source;
    }
    
    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherFilterSubscriber<>(s, predicate));
    }
    
    static final class PublisherFilterSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final Predicate<? super T> predicate;

        Subscription s;
        
        boolean done;
        
        public PublisherFilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
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
            
            boolean b;
            
            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                
                onError(e);
                return;
            }
            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
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
