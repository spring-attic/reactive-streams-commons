package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.error.ExceptionHelper;
import reactivestreams.commons.subscriber.SubscriberMultiSubscription;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 */
public final class PublisherRetryPredicate<T> extends PublisherSource<T, T> {

    final Predicate<Throwable> predicate;

    public PublisherRetryPredicate(Publisher<? extends T> source, Predicate<Throwable> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRetryPredicateSubscriber<T> parent = new PublisherRetryPredicateSubscriber<>(source, s, predicate);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.resubscribe();
        }
    }

    static final class PublisherRetryPredicateSubscriber<T>
      extends SubscriberMultiSubscription<T, T> {

        final Publisher<? extends T> source;

        final Predicate<Throwable> predicate;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRetryPredicateSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherRetryPredicateSubscriber.class, "wip");

        long produced;

        public PublisherRetryPredicateSubscriber(Publisher<? extends T> source, 
                Subscriber<? super T> actual, Predicate<Throwable> predicate) {
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
        public void onError(Throwable t) {
            boolean b;
            
            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                Throwable _t = ExceptionHelper.unwrap(e);
                _t.addSuppressed(t);
                subscriber.onError(_t);
                return;
            }
            
            if (b) {
                resubscribe();
            } else {
                subscriber.onError(t);
            }
        }
        
        @Override
        public void onComplete() {
            
            subscriber.onComplete();
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
