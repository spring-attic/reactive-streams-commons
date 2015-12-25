package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;

/**
 * Repeatedly subscribes to the source sequence if it signals any error
 * either indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite retry.
 *
 * @param <T> the value type
 */
public final class PublisherRetry<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final long times;

    public PublisherRetry(Publisher<? extends T> source) {
        this(source, Long.MAX_VALUE);
    }

    public PublisherRetry(Publisher<? extends T> source, long times) {
        if (times < 0L) {
            throw new IllegalArgumentException("times >= 0 required");
        }
        this.source = Objects.requireNonNull(source, "source");
        this.times = times;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherRetrySubscriber<T> parent = new PublisherRetrySubscriber<>(source, s, times);

        s.onSubscribe(parent.arbiter);
        
        if (!parent.arbiter.isCancelled()) {
            parent.resubscribe();
        }
    }
    
    static final class PublisherRetrySubscriber<T> 
    implements Subscriber<T> {

        final Subscriber<? super T> actual;

        final Publisher<? extends T> source;
        
        final MultiSubscriptionArbiter arbiter;
        
        long remaining;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRetrySubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherRetrySubscriber.class, "wip");

        public PublisherRetrySubscriber(Publisher<? extends T> source, Subscriber<? super T> actual, long remaining) {
            this.source = source;
            this.actual = actual;
            this.remaining = remaining;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
            
            arbiter.producedOne();
        }

        @Override
        public void onError(Throwable t) {
            long r = remaining;
            if (r != Long.MAX_VALUE) {
                if (r == 0) {
                    actual.onError(t);
                    return;
                }
                remaining = r - 1;
            }
            
            resubscribe();
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (arbiter.isCancelled()) {
                        return;
                    }
                    source.subscribe(this);
                    
                } while (WIP.decrementAndGet(this) != 0);
            }
        }
    }
}
