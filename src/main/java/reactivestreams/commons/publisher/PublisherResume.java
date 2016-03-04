package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Loopback;
import reactivestreams.commons.subscriber.MultiSubscriptionSubscriber;
import reactivestreams.commons.util.ExceptionHelper;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 */
public final class PublisherResume<T> extends PublisherSource<T, T> {

    final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

// FIXME this causes ambiguity error because javac can't distinguish between different lambda arities:
//
//    new PublisherResume(source, e -> other) tries to match the (Publisher, Publisher) constructor and fails
//
//    public PublisherResume(Publisher<? extends T> source,
//            Publisher<? extends T> next) {
//        this(source, create(next));
//    }
//
    static <T> Function<Throwable, Publisher<? extends T>> create(final Publisher<? extends T> next) {
        Objects.requireNonNull(next, "next");
        return e -> next;
    }

    @Override
    public long getCapacity() {
        return -1L;
    }

    public static <T> PublisherResume<T> create(Publisher<? extends T> source, Publisher<? extends T> other) {
        return new PublisherResume<>(source, create(other));
    }
    
    public PublisherResume(Publisher<? extends T> source,
                           Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
        super(source);
        this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherResumeSubscriber<>(s, nextFactory));
    }

    static final class PublisherResumeSubscriber<T> extends MultiSubscriptionSubscriber<T, T>
            implements Loopback {

        final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

        boolean second;

        public PublisherResumeSubscriber(Subscriber<? super T> actual,
                                         Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
            super(actual);
            this.nextFactory = nextFactory;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!second) {
                subscriber.onSubscribe(this);
            }
            set(s);
        }

        @Override
        public void onNext(T t) {
            subscriber.onNext(t);

            if (!second) {
                producedOne();
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!second) {
                second = true;

                Publisher<? extends T> p;

                try {
                    p = nextFactory.apply(t);
                } catch (Throwable e) {
                    Throwable _e = ExceptionHelper.unwrap(e);
                    _e.addSuppressed(t);
                    ExceptionHelper.throwIfFatal(e);
                    subscriber.onError(_e);
                    return;
                }
                if (p == null) {
                    NullPointerException t2 = new NullPointerException("The nextFactory returned a null Publisher");
                    t2.addSuppressed(t);
                    subscriber.onError(t2);
                } else {
                    p.subscribe(this);
                }
            } else {
                subscriber.onError(t);
            }
        }

        @Override
        public Object connectedInput() {
            return nextFactory;
        }

        @Override
        public Object connectedOutput() {
            return null;
        }
    }
}
