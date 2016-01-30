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

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.Loopback;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */
public final class PublisherFilterFuseable<T> extends PublisherSource<T, T>
        implements Fuseable {

    final Predicate<? super T> predicate;

    public PublisherFilterFuseable(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        if (!(source instanceof Fuseable)) {
            throw new IllegalArgumentException("The source must implement the Fuseable interface for this operator to work");
        }
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new PublisherFilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, predicate));
            return;
        }
        source.subscribe(new PublisherFilterFuseableSubscriber<>(s, predicate));
    }

    static final class PublisherFilterFuseableSubscriber<T> 
    extends SynchronousSubscription<T>
    implements Receiver, Producer, Loopback, Completable, Subscription, ConditionalSubscriber<T> {
        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        QueueSubscription<T> s;

        boolean done;
        
        int sourceMode;

        /** Running with regular, arbitrary source. */
        static final int NORMAL = 0;
        /** Running with a source that implements SynchronousSource. */
        static final int SYNC = 1;
        /** Running with a source that implements AsynchronousSource. */
        static final int ASYNC = 2;
        
        public PublisherFilterFuseableSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = (QueueSubscription<T>)s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            int m = sourceMode;
            
            if (m == 0) {
                boolean b;
    
                try {
                    b = predicate.test(t);
                } catch (Throwable e) {
                    s.cancel();
    
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
                if (b) {
                    actual.onNext(t);
                } else {
                    s.request(1);
                }
            } else
            if (m == 2) {
                actual.onNext(null);
            }
        }
        
        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return false;
            }

            int m = sourceMode;
            
            if (m == 0) {
                boolean b;
    
                try {
                    b = predicate.test(t);
                } catch (Throwable e) {
                    s.cancel();
    
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return false;
                }
                if (b) {
                    actual.onNext(t);
                    return true;
                }
                return false;
            } else
            if (m == 2) {
                actual.onNext(null);
            }
            return true;
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
        public Object connectedInput() {
            return predicate;
        }

        @Override
        public Object connectedOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public T poll() {
            if (sourceMode == ASYNC) {
                long dropped = 0;
                for (;;) {
                    T v = s.poll();
    
                    if (v == null || predicate.test(v)) {
                        if (dropped != 0) {
                            request(dropped);
                        }
                        return v;
                    }
                    dropped++;
                }
            } else {
                for (;;) {
                    T v = s.poll();
    
                    if (v == null || predicate.test(v)) {
                        return v;
                    }
                }
            }
        }

        @Override
        public T peek() {
            if (sourceMode == ASYNC) {
                long dropped = 0;
                for (;;) {
                    T v = s.peek();
    
                    if (v == null || predicate.test(v)) {
                        if (dropped != 0) {
                            request(dropped);
                        }
                        return v;
                    }
                    s.drop();
                    dropped++;
                }
            } else {
                for (;;) {
                    T v = s.peek();
    
                    if (v == null || predicate.test(v)) {
                        return v;
                    }
                    s.drop();
                }
            }
        }

        @Override
        public boolean isEmpty() {
            return peek() == null;
        }

        @Override
        public void clear() {
            s.clear();
        }
        
        @Override
        public void drop() {
            s.drop();
        }
        
        @Override
        public FusionMode requestFusion(FusionMode requestedMode) {
            FusionMode m = s.requestFusion(requestedMode);
            if (m != FusionMode.NONE) {
                sourceMode = m == FusionMode.SYNC ? SYNC : ASYNC;
            }
            return m;
        }
    }

    static final class PublisherFilterFuseableConditionalSubscriber<T> 
    extends SynchronousSubscription<T>
    implements Receiver, Producer, Loopback, Completable, Subscription, ConditionalSubscriber<T> {
        final ConditionalSubscriber<? super T> actual;

        final Predicate<? super T> predicate;

        QueueSubscription<T> s;

        boolean done;
        
        int sourceMode;

        /** Running with regular, arbitrary source. */
        static final int NORMAL = 0;
        /** Running with a source that implements SynchronousSource. */
        static final int SYNC = 1;
        /** Running with a source that implements AsynchronousSource. */
        static final int ASYNC = 2;
        
        public PublisherFilterFuseableConditionalSubscriber(ConditionalSubscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = (QueueSubscription<T>)s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            int m = sourceMode;
            
            if (m == 0) {
                boolean b;
    
                try {
                    b = predicate.test(t);
                } catch (Throwable e) {
                    s.cancel();
    
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
                if (b) {
                    actual.onNext(t);
                } else {
                    s.request(1);
                }
            } else
            if (m == 2) {
                actual.onNext(null);
            }
        }
        
        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return false;
            }

            int m = sourceMode;
            
            if (m == 0) {
                boolean b;
    
                try {
                    b = predicate.test(t);
                } catch (Throwable e) {
                    s.cancel();
    
                    ExceptionHelper.throwIfFatal(e);
                    onError(ExceptionHelper.unwrap(e));
                    return false;
                }
                if (b) {
                    return actual.tryOnNext(t);
                }
                return false;
            } else
            if (m == 2) {
                actual.onNext(null);
            }
            return true;
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
        public Object connectedInput() {
            return predicate;
        }

        @Override
        public Object connectedOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public T poll() {
            if (sourceMode == ASYNC) {
                long dropped = 0;
                for (;;) {
                    T v = s.poll();
    
                    if (v == null || predicate.test(v)) {
                        if (dropped != 0) {
                            request(dropped);
                        }
                        return v;
                    }
                    dropped++;
                }
            } else {
                for (;;) {
                    T v = s.poll();
    
                    if (v == null || predicate.test(v)) {
                        return v;
                    }
                }
            }
        }

        @Override
        public T peek() {
            if (sourceMode == ASYNC) {
                long dropped = 0;
                for (;;) {
                    T v = s.peek();
    
                    if (v == null || predicate.test(v)) {
                        if (dropped != 0) {
                            request(dropped);
                        }
                        return v;
                    }
                    s.drop();
                    dropped++;
                }
            } else {
                for (;;) {
                    T v = s.peek();
    
                    if (v == null || predicate.test(v)) {
                        return v;
                    }
                    s.drop();
                }
            }
        }

        @Override
        public boolean isEmpty() {
            return peek() == null;
        }

        @Override
        public void clear() {
            s.clear();
        }
        
        @Override
        public void drop() {
            s.drop();
        }
        
        @Override
        public FusionMode requestFusion(FusionMode requestedMode) {
            FusionMode m = s.requestFusion(requestedMode);
            if (m != FusionMode.NONE) {
                sourceMode = m == FusionMode.SYNC ? SYNC : ASYNC;
            }
            return m;
        }
    }

}
