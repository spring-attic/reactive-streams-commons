package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.*;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 */
public final class PublisherSwitchIfEmpty<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Publisher<? extends T> other;

    public PublisherSwitchIfEmpty(Publisher<? extends T> source, Publisher<? extends T> other) {
        this.source = Objects.requireNonNull(source, "source");
        this.other = Objects.requireNonNull(other, "other");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherSwitchIfEmptySubscriber<T> parent = new PublisherSwitchIfEmptySubscriber<>(s, other);
        
        s.onSubscribe(parent.arbiter);
        
        source.subscribe(parent);
    }
    
    static final class PublisherSwitchIfEmptySubscriber<T> implements Subscriber<T> {
        
        final Subscriber<? super T> actual;
        
        final Publisher<? extends T> other;

        final MultiSubscriptionArbiter arbiter;

        boolean once;
        
        public PublisherSwitchIfEmptySubscriber(Subscriber<? super T> actual, Publisher<? extends T> other) {
            this.actual = actual;
            this.other = other;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            if (!once) {
                once = true;
            }
            
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (!once) {
                once = true;
                
                other.subscribe(this);
            } else {
                actual.onComplete();
            }
        }
        
        
    }
}
