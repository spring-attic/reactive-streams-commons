package reactivestreams.commons;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscriber.SubscriberDeferScalar;
import reactivestreams.commons.internal.support.SubscriptionHelper;
import reactivestreams.commons.internal.subscription.EmptySubscription;

/**
 * Aggregates the source values with the help of an accumulator 
 * function and emits the the final accumulated value.
 *
 * @param <T> the source value type
 * @param <R> the accumulated result type
 */
public final class PublisherReduce<T, R> implements Publisher<R> {

    final Publisher<? extends T> source;
    
    final Supplier<R> initialSupplier;
    
    final BiFunction<R, ? super T, R> accumulator;

    public PublisherReduce(Publisher<? extends T> source, Supplier<R> initialSupplier,
            BiFunction<R, ? super T, R> accumulator) {
        this.source = Objects.requireNonNull(source, "source");
        this.initialSupplier = Objects.requireNonNull(initialSupplier, "initialSupplier");
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        R initialValue;

        try {
        initialValue = initialSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (initialValue == null) {
            EmptySubscription.error(s, new NullPointerException("The initial value supplied is null"));
            return;
        }
        
        source.subscribe(new PublisherReduceSubscriber<>(s, accumulator, initialValue));
    }
    
    static final class PublisherReduceSubscriber<T, R>
            extends SubscriberDeferScalar<T, R> {

        final BiFunction<R, ? super T, R> accumulator;

        Subscription s;
        
        boolean done;

        public PublisherReduceSubscriber(Subscriber<? super R> actual, BiFunction<R, ? super T, R> accumulator,
                R value) {
            super(actual);
            this.accumulator = accumulator;
            this.value = value;
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        @Override
        public void sdsSetValue(R value) {
            // value already saved
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
            R v;
            
            try {
                v = accumulator.apply(value, t);
            } catch (Throwable e) {
                cancel();
                
                onError(e);
                return;
            }
            
            if (v == null) {
                cancel();
                
                onError(new NullPointerException("The accumulator returned a null value"));
                return;
            }
            
            value = v;
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

            sdsSet(value);
        }
        
        
    }
}
