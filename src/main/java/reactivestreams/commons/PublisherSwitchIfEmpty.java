package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
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
        
        s.onSubscribe(parent);
        
        source.subscribe(parent);
    }
    
    static final class PublisherSwitchIfEmptySubscriber<T> extends MultiSubscriptionArbiter<T> {
        
        final Publisher<? extends T> other;

        boolean once;
        
        public PublisherSwitchIfEmptySubscriber(Subscriber<? super T> actual, Publisher<? extends T> other) {
            super(actual);
            this.other = other;
        }

        @Override
        public void onNext(T t) {
            if (!once) {
                once = true;
            }
            
            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (!once) {
                once = true;
                
                other.subscribe(this);
            } else {
                subscriber.onComplete();
            }
        }
        
        
    }
}
