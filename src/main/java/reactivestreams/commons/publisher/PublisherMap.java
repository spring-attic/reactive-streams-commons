package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.*;

import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.support.SubscriptionHelper;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
public final class PublisherMap<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends R> mapper;

    /**
     * Constructs a PublisherMap instance with the given source and mapper.
     *
     * @param source the source Publisher instance
     * @param mapper the mapper function
     * @throws NullPointerException if either {@code source} or {@code mapper} is null.
     */
    public PublisherMap(Publisher<? extends T> source, Function<? super T, ? extends R> mapper) {
        super(source);
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public Function<? super T, ? extends R> mapper() {
        return mapper;
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        source.subscribe(new PublisherMapSubscriber<>(s, mapper));
    }

    static final class PublisherMapSubscriber<T, R> implements Subscriber<T>,
                                                               Upstream, Downstream, FeedbackLoop, ActiveUpstream{
        final Subscriber<? super R>            actual;
        final Function<? super T, ? extends R> mapper;

        boolean done;

        Subscription s;

        public PublisherMapSubscriber(Subscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
            this.actual = actual;
            this.mapper = mapper;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(s);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            R v;

            try {
                v = mapper.apply(t);
            } catch (Throwable e) {
                done = true;
                s.cancel();
                actual.onError(e);
                return;
            }

            if (v == null) {
                done = true;
                s.cancel();
                actual.onError(new NullPointerException("The mapper returned a null value."));
                return;
            }

            actual.onNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
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

            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object delegateInput() {
            return mapper;
        }

        @Override
        public Object delegateOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
