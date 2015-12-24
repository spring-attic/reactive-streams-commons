package reactivestreams.commons;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Relays values from the main Publisher until another Publisher signals an event.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 */
public final class PublisherTakeUntil<T, U> implements Publisher<T> {
    
    final Publisher<? extends T> source;
    
    final Publisher<U> other;

    public PublisherTakeUntil(Publisher<? extends T> source, Publisher<U> other) {
        this.source = source;
        this.other = other;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
     // TODO Auto-generated method stub
    }
}
