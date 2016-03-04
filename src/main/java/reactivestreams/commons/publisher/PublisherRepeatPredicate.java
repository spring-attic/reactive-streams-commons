package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscriber.MultiSubscriptionSubscriber;
import reactivestreams.commons.util.ExceptionHelper;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 */
public final class PublisherRepeatPredicate<T> extends PublisherSource<T, T> {

    final BooleanSupplier predicate;

    public PublisherRepeatPredicate(Publisher<? extends T> source, BooleanSupplier predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRepeatPredicateSubscriber<T> parent = new PublisherRepeatPredicateSubscriber<>(source, s, predicate);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.resubscribe();
        }
    }

    @Override
    public long getCapacity() {
        return -1L;
    }

    static final class PublisherRepeatPredicateSubscriber<T>
            extends MultiSubscriptionSubscriber<T, T> {

        final Publisher<? extends T> source;

        final BooleanSupplier predicate;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRepeatPredicateSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherRepeatPredicateSubscriber.class, "wip");

        long produced;

        public PublisherRepeatPredicateSubscriber(Publisher<? extends T> source, 
                Subscriber<? super T> actual, BooleanSupplier predicate) {
            super(actual);
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            boolean b;
            
            try {
                b = predicate.getAsBoolean();
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                subscriber.onError(ExceptionHelper.unwrap(e));
                return;
            }
            
            if (b) {
                resubscribe();
            } else {
                subscriber.onComplete();
            }
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
