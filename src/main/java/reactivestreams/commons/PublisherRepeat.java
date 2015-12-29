package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Repeatedly subscribes to the source and relays its values either
 * indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite repeat.
 *
 * @param <T> the value type
 */
public final class PublisherRepeat<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final long times;

    public PublisherRepeat(Publisher<? extends T> source) {
        this(source, Long.MAX_VALUE);
    }

    public PublisherRepeat(Publisher<? extends T> source, long times) {
        if (times < 0L) {
            throw new IllegalArgumentException("times >= 0 required");
        }
        this.source = Objects.requireNonNull(source, "source");
        this.times = times;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (times == 0) {
            EmptySubscription.complete(s);
            return;
        }
        
        PublisherRepeatSubscriber<T> parent = new PublisherRepeatSubscriber<>(source, s, times);

        s.onSubscribe(parent);
        
        if (!parent.isCancelled()) {
            parent.onComplete();
        }
    }
    
    static final class PublisherRepeatSubscriber<T> 
    extends MultiSubscriptionArbiter<T, T> {

        final Publisher<? extends T> source;
        
        long remaining;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRepeatSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherRepeatSubscriber.class, "wip");

        long produced;
        
        public PublisherRepeatSubscriber(Publisher<? extends T> source, Subscriber<? super T> actual, long remaining) {
            super(actual);
            this.source = source;
            this.remaining = remaining;
        }

        @Override
        public void onNext(T t) {
            produced++;
            
            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            long r = remaining;
            if (r != Long.MAX_VALUE) {
                if (r == 0) {
                    subscriber.onComplete();
                    return;
                }
                remaining = r - 1;
            }
            
            resubscribe();
        }
        
        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (isCancelled()) {
                        return;
                    }
                    
                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        produced(c);
                    }

                    source.subscribe(this);
                    
                } while (WIP.decrementAndGet(this) != 0);
            }
        }
    }
}
