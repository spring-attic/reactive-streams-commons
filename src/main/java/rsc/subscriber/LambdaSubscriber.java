package rsc.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.Cancellation;
import rsc.util.ExceptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * An subscriber that forwards onXXX calls to lambdas and allows asynchronous cancellation
 * via run().
 *
 * @param <T> the value type
 */
public final class LambdaSubscriber<T> implements Subscriber<T>, Cancellation {

    final Consumer<? super T> onNextCall;
    
    final Consumer<Throwable> onErrorCall;
    
    final Runnable onCompleteCall;
    
    volatile Subscription s;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<LambdaSubscriber, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(LambdaSubscriber.class, Subscription.class, "s");
    
    boolean done;
    
    public LambdaSubscriber(Consumer<? super T> onNextCall, Consumer<Throwable> onErrorCall, Runnable onCompleteCall) {
        this.onNextCall = Objects.requireNonNull(onNextCall, "onNextCall");
        this.onErrorCall = Objects.requireNonNull(onErrorCall, "onErrorCall");
        this.onCompleteCall = Objects.requireNonNull(onCompleteCall, "onCompleteCall");
    }

    @Override
    public void dispose() {
        SubscriptionHelper.terminate(S, this);
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.setOnce(S, this, s)) {
            s.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(T t) {
        try {
            onNextCall.accept(t);
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            dispose();
            onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (done) {
            UnsignalledExceptions.onErrorDropped(t);
            return;
        }
        done = true;
        try {
            onErrorCall.accept(t);
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            e.addSuppressed(t);
            UnsignalledExceptions.onErrorDropped(e);
        }
    }

    @Override
    public void onComplete() {
        if (done) {
            return;
        }
        done = true;
        try {
            onCompleteCall.run();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            UnsignalledExceptions.onErrorDropped(e);
        }
    }

}
