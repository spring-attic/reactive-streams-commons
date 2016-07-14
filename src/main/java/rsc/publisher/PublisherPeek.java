package rsc.publisher;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.flow.Fuseable.ConditionalSubscriber;
import rsc.publisher.PublisherPeekFuseable.PublisherPeekConditionalSubscriber;
import rsc.publisher.PublisherPeekFuseable.PublisherPeekFuseableSubscriber;

import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

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
@FusionSupport(input = { FusionMode.CONDITIONAL }, output = { FusionMode.CONDITIONAL })
public final class PublisherPeek<T> extends PublisherSource<T, T>
implements PublisherPeekHelper<T> {

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
        if (source instanceof Fuseable) {
            source.subscribe(new PublisherPeekFuseableSubscriber<>(s, this));
            return;
        }
        if (s instanceof ConditionalSubscriber) {
            @SuppressWarnings("unchecked") // javac, give reason to suppress because inference anomalies
            ConditionalSubscriber<T> s2 = (ConditionalSubscriber<T>)s;
            source.subscribe(new PublisherPeekConditionalSubscriber<>(s2, this));
            return;
        }
        source.subscribe(new PublisherPeekSubscriber<>(s, this));
    }
    
    static final class PublisherPeekSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer {

        final Subscriber<? super T> actual;

        final PublisherPeekHelper<T> parent;

        Subscription s;

        boolean done;
        
        public PublisherPeekSubscriber(Subscriber<? super T> actual, PublisherPeekHelper<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            if(parent.onRequestCall() != null) {
                try {
                    parent.onRequestCall().accept(n);
                }
                catch (Throwable e) {
                    cancel();
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            if(parent.onCancelCall() != null) {
                try {
                    parent.onCancelCall().run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    s.cancel();
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
            }
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if(parent.onSubscribeCall() != null) {
                try {
                    parent.onSubscribeCall().accept(s);
                }
                catch (Throwable e) {
                    s.cancel();
                    actual.onSubscribe(SubscriptionHelper.empty());
                    onError(e);
                    return;
                }
            }
            this.s = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            if(parent.onNextCall() != null) {
                try {
                    parent.onNextCall().accept(t);
                }
                catch (Throwable e) {
                    cancel();
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
            }

            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            if(parent.onErrorCall() != null) {
                ExceptionHelper.throwIfFatal(t);
                parent.onErrorCall().accept(t);
            }

            actual.onError(t);

            if(parent.onAfterTerminateCall() != null) {
                try {
                    parent.onAfterTerminateCall().run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    Throwable _e = ExceptionHelper.unwrap(e);
                    e.addSuppressed(ExceptionHelper.unwrap(t));
                    if(parent.onErrorCall() != null) {
                        parent.onErrorCall().accept(_e);
                    }
                    actual.onError(_e);
                }
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            if(parent.onCompleteCall() != null) {
                try {
                    parent.onCompleteCall().run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
            }

            actual.onComplete();

            if(parent.onAfterTerminateCall() != null) {
                try {
                    parent.onAfterTerminateCall().run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    Throwable _e = ExceptionHelper.unwrap(e);
                    if(parent.onErrorCall() != null) {
                        parent.onErrorCall().accept(_e);
                    }
                    actual.onError(_e);
                }
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

    @Override
    public Consumer<? super Subscription> onSubscribeCall() {
        return onSubscribeCall;
    }



    @Override
    public Consumer<? super T> onNextCall() {
        return onNextCall;
    }



    @Override
    public Consumer<? super Throwable> onErrorCall() {
        return onErrorCall;
    }



    @Override
    public Runnable onCompleteCall() {
        return onCompleteCall;
    }



    @Override
    public Runnable onAfterTerminateCall() {
        return onAfterTerminateCall;
    }



    @Override
    public LongConsumer onRequestCall() {
        return onRequestCall;
    }



    @Override
    public Runnable onCancelCall() {
        return onCancelCall;
    }

}
