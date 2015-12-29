package reactivestreams.commons;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.support.BackpressureHelper;
import reactivestreams.commons.internal.subscriber.SubscriberDeferScalar;
import reactivestreams.commons.internal.support.SubscriptionHelper;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 */
public final class PublisherTakeLast<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final int n;

    public PublisherTakeLast(Publisher<? extends T> source, int n) {
        if (n < 0) {
            throw new IllegalArgumentException("n >= required but it was " + n);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.n = n;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (n == 0) {
            source.subscribe(new PublisherTakeLastZeroSubscriber<>(s));
        } else
        if (n == 1) {
            source.subscribe(new PublisherTakeLastOneSubscriber<>(s));
        } else {
            source.subscribe(new PublisherTakeLastManySubscriber<>(s, n));
        }
    }
    
    static final class PublisherTakeLastZeroSubscriber<T> implements Subscriber<T> {
        
        final Subscriber<? super T> actual;

        public PublisherTakeLastZeroSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            actual.onSubscribe(s);
            
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            // ignoring all values
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }

    static final class PublisherTakeLastOneSubscriber<T>
            extends SubscriberDeferScalar<T, T> {

        Subscription s;

       public PublisherTakeLastOneSubscriber(Subscriber<? super T> actual) {
            super(actual);
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
            value = t;
        }

        @Override
        public void onComplete() {
            T v = value;
            if (v == null) {
                subscriber.onComplete();
            }
            set(v);
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        @Override
        public void setValue(T value) {
            // value is always in a field
        }
    }

    static final class PublisherTakeLastManySubscriber<T> 
    implements Subscriber<T>, Subscription, BooleanSupplier {
        
        final Subscriber<? super T> actual;
        
        final int n;
        
        volatile boolean cancelled;
        
        Subscription s;
        
        final ArrayDeque<T> buffer;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherTakeLastManySubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherTakeLastManySubscriber.class, "requested");
        
        public PublisherTakeLastManySubscriber(Subscriber<? super T> actual, int n) {
            this.actual = actual;
            this.n = n;
            this.buffer = new ArrayDeque<>();
        }

        @Override
        public boolean getAsBoolean() {
            return cancelled;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.postCompleteRequest(n, actual, buffer, REQUESTED, this, this);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
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
            ArrayDeque<T> bs = buffer;
            
            if (bs.size() == n) {
                bs.poll();
            }
            bs.offer(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            
            BackpressureHelper.postComplete(actual, buffer, REQUESTED, this, this);
        }
    }
}
