package reactivestreams.commons.publisher;

import org.reactivestreams.*;

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
    
    static final class PublisherIgnoreElementsSubscriber<T> implements Subscriber<T>, Downstream {
        final Subscriber<? super T> actual;
        
        public PublisherIgnoreElementsSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            actual.onSubscribe(s);
            s.request(Long.MAX_VALUE);
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
    }
}
