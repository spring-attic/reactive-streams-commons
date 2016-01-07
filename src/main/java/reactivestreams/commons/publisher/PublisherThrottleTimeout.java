package reactivestreams.commons.publisher;

import java.util.function.Function;

import org.reactivestreams.*;

/**
 * Emits the last value from upstream only if there were no newer values emitted
 * during the time window provided by a publisher for that particular last value.
 *
 * @param <T> the source value type
 * @param <U> the value type of the duration publisher
 */
public final class PublisherThrottleTimeout<T, U> extends PublisherSource<T, T> {

    final Function<? super T, ? extends Publisher<U>> throttler;

    public PublisherThrottleTimeout(Publisher<? extends T> source,
            Function<? super T, ? extends Publisher<U>> throttler) {
        super(source);
        this.throttler = throttler;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        // TODO Auto-generated method stub
        
    }
}
