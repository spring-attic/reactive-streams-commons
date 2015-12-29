package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactivestreams.commons.internal.subscription.EmptySubscription;

/**
 * maps the downstream Subscriber into an upstream Subscriber 
 * which allows implementing custom operators via lambdas.
 *
 * @param <T> the upstream value type
 * @param <R> the downstream value type
 */
public final class PublisherLift<T, R> implements Publisher<R> {

    final Publisher<? extends T> source;
    
    final Function<Subscriber<? super R>, Subscriber<? super T>> lifter;

    public PublisherLift(Publisher<? extends T> source, Function<Subscriber<? super R>, Subscriber<? super T>> lifter) {
        this.source = Objects.requireNonNull(source, "source");
        this.lifter = Objects.requireNonNull(lifter, "lifter");
    }
    
    public Function<Subscriber<? super R>, Subscriber<? super T>> lifter() {
        return lifter;
    }
    
    public Publisher<? extends T> source() {
        return source;
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {

        Subscriber<? super T> ts;
        try {
            ts = lifter.apply(s);
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (ts == null) {
            EmptySubscription.error(s, new NullPointerException("The lifter returned a null Subscriber"));
            return;
        }
        
        source.subscribe(ts);
    }
}
