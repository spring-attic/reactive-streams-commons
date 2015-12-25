package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscriptionTrait;

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
    implements Subscriber<T>, ScalarDelayedSubscriptionTrait<R> {

        final Subscriber<? super R> actual;
        
        final BiFunction<R, ? super T, R> accumulator;

        R value;
        
        Subscription s;
        
        boolean done;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherReduceSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherReduceSubscriber.class, "wip");

        public PublisherReduceSubscriber(Subscriber<? super R> actual, BiFunction<R, ? super T, R> accumulator,
                R value) {
            this.actual = actual;
            this.accumulator = accumulator;
            this.value = value;
        }

        @Override
        public void request(long n) {
            ScalarDelayedSubscriptionTrait.super.request(n);
        }

        @Override
        public void cancel() {
            ScalarDelayedSubscriptionTrait.super.cancel();

            s.cancel();
        }

        @Override
        public int sdsGetState() {
            return wip;
        }

        @Override
        public void sdsSetState(int updated) {
            wip = updated;
        }

        @Override
        public boolean sdsCasState(int expected, int updated) {
            return WIP.compareAndSet(this, expected, updated);
        }

        @Override
        public R sdsGetValue() {
            return value;
        }

        @Override
        public void sdsSetValue(R value) {
            // value already saved
        }

        @Override
        public Subscriber<? super R> sdsGetSubscriber() {
            return actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
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
            
            actual.onError(t);
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
