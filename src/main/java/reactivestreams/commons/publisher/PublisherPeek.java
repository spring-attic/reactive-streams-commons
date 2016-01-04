package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.error.UnsignalledExceptions;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 */
public final class PublisherPeek<T> extends PublisherSource<T, T> {

    final Consumer<? super Subscription> onSubscribeCall;

    final Consumer<? super T> onNextCall;

    final Consumer<? super Throwable> onErrorCall;

    final Runnable onCompleteCall;

    final Runnable onAfterTerminateCall;

    final LongConsumer onRequestCall;

    final Runnable onCancelCall;

    public PublisherPeek(Publisher<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
                         Consumer<? super T> onNextCall, Consumer<? super Throwable> onErrorCall, Runnable
                           onCompleteCall,
                         Runnable onAfterTerminateCall, LongConsumer onRequestCall, Runnable onCancelCall) {
        super(source);
        this.onSubscribeCall = onSubscribeCall;
        this.onNextCall = onNextCall;
        this.onErrorCall = onErrorCall;
        this.onCompleteCall = onCompleteCall;
        this.onAfterTerminateCall = onAfterTerminateCall;
        this.onRequestCall = onRequestCall;
        this.onCancelCall = onCancelCall;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherPeekSubscriber<>(s, this));
    }

    static final class PublisherPeekSubscriber<T> implements Subscriber<T>, Subscription,
                                                             Upstream, Downstream {

        final Subscriber<? super T> actual;

        final PublisherPeek<T> parent;

        Subscription s;

        public PublisherPeekSubscriber(Subscriber<? super T> actual, PublisherPeek<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            try {
                parent.onRequestCall.accept(n);
            } catch (Throwable e) {
                cancel();
                onError(e);
                return;
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            try {
                parent.onCancelCall.run();
            } catch (Throwable e) {
                cancel();
                onError(e);
                return;
            }
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            try {
                parent.onSubscribeCall.accept(s);
            } catch (Throwable e) {
                onError(e);
                EmptySubscription.error(actual, e);
                return;
            }
            this.s = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            try {
                parent.onNextCall.accept(t);
            } catch (Throwable e) {
                cancel();
                onError(e);
                return;
            }
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            UnsignalledExceptions.throwIfFatal(t);
            try {
                parent.onErrorCall.accept(t);
            } catch (Throwable e) {
                UnsignalledExceptions.onErrorDropped(e);
                return;
            }

            actual.onError(t);

            try {
                parent.onAfterTerminateCall.run();
            } catch (Throwable e) {
                UnsignalledExceptions.throwIfFatal(e);
                parent.onErrorCall.accept(t);
                actual.onError(e);
            }
        }

        @Override
        public void onComplete() {
            try {
                parent.onCompleteCall.run();
            } catch (Throwable e) {
                onError(e);
                return;
            }

            actual.onComplete();

            try {
                parent.onAfterTerminateCall.run();
            } catch (Throwable e) {
                UnsignalledExceptions.throwIfFatal(e);
                parent.onErrorCall.accept(e);
                actual.onError(e);
            }
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
