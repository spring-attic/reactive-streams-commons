/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.graph.Publishable;
import reactivestreams.commons.graph.Subscribable;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;

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

    static final class PublisherPeekSubscriber<T> implements Subscriber<T>, Subscription, Publishable, Subscribable {

        final Subscriber<? super T> actual;

        final PublisherPeek<T> parent;

        Subscription s;

        public PublisherPeekSubscriber(Subscriber<? super T> actual, PublisherPeek<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            if(parent.onRequestCall != null) {
                try {
                    parent.onRequestCall.accept(n);
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
            if(parent.onCancelCall != null) {
                try {
                    parent.onCancelCall.run();
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
            if(parent.onSubscribeCall != null) {
                try {
                    parent.onSubscribeCall.accept(s);
                }
                catch (Throwable e) {
                    onError(e);
                    EmptySubscription.error(actual, ExceptionHelper.unwrap(e));
                    return;
                }
            }
            this.s = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if(parent.onNextCall != null) {
                try {
                    parent.onNextCall.accept(t);
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
            if(parent.onErrorCall != null) {
                ExceptionHelper.throwIfFatal(t);
                parent.onErrorCall.accept(t);
            }

            actual.onError(t);

            if(parent.onAfterTerminateCall != null) {
                try {
                    parent.onAfterTerminateCall.run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    Throwable _e = ExceptionHelper.unwrap(e);
                    e.addSuppressed(ExceptionHelper.unwrap(t));
                    if(parent.onErrorCall != null) {
                        parent.onErrorCall.accept(_e);
                    }
                    actual.onError(_e);
                }
            }
        }

        @Override
        public void onComplete() {
            if(parent.onCompleteCall != null) {
                try {
                    parent.onCompleteCall.run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
            }

            actual.onComplete();

            if(parent.onAfterTerminateCall != null) {
                try {
                    parent.onAfterTerminateCall.run();
                }
                catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    Throwable _e = ExceptionHelper.unwrap(e);
                    if(parent.onErrorCall != null) {
                        parent.onErrorCall.accept(_e);
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
}
