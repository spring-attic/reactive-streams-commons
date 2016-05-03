package rsc.publisher;

import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Aggregates the source items with an aggregator function and returns the last result.
 *
 * @param <T> the input and output value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherAggregate<T> extends PublisherSource<T, T> implements Fuseable {

    final BiFunction<T, T, T> aggregator;

    public PublisherAggregate(Publisher<? extends T> source, BiFunction<T, T, T> aggregator) {
        super(source);
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherAggregateSubscriber<>(s, aggregator));
    }
    
    static final class PublisherAggregateSubscriber<T> extends DeferredScalarSubscriber<T, T> {
        final BiFunction<T, T, T> aggregator;

        Subscription s;
        
        T result;
        
        boolean done;
        
        public PublisherAggregateSubscriber(Subscriber<? super T> actual, BiFunction<T, T, T> aggregator) {
            super(actual);
            this.aggregator = aggregator;
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
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            T r = result;
            if (r == null) {
                result = t;
            } else {
                try {
                    r = aggregator.apply(r, t);
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    s.cancel();
                    result = null;
                    done = true;
                    subscriber.onError(ex);
                    return;
                }
                
                if (r == null) {
                    s.cancel();
                    result = null;
                    done = true;
                    subscriber.onError(new NullPointerException("The aggregator returned a null value"));
                    return;
                }
                
                result = r;
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            result = null;
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            T r = result;
            if (r != null) {
                complete(r);
            } else {
                subscriber.onComplete();
            }
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
