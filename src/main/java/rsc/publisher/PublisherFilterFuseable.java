package rsc.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.*;
import rsc.state.Completable;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL }, output = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL, FusionMode.BOUNDARY })
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
    implements Receiver, Producer, Loopback, Completable, SynchronousSubscription<T>, ConditionalSubscriber<T> {
        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        QueueSubscription<T> s;

        boolean done;
        
        int sourceMode;

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
                    ExceptionHelper.throwIfFatal(e);
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
                    ExceptionHelper.throwIfFatal(e);
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
        public boolean isEmpty() {
            return s.isEmpty();
        }

        @Override
        public void clear() {
            s.clear();
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            int m;
            if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
                if ((requestedMode & Fuseable.SYNC) != 0) {
                    m = s.requestFusion(Fuseable.SYNC);
                } else {
                    m = Fuseable.NONE;
                }
            } else {
                m = s.requestFusion(requestedMode);
            }
            sourceMode = m;
            return m;
        }
        
        @Override
        public int size() {
            return s.size();
        }
    }

    static final class PublisherFilterFuseableConditionalSubscriber<T> 
    implements Receiver, Producer, Loopback, Completable, ConditionalSubscriber<T>, SynchronousSubscription<T> {
        final ConditionalSubscriber<? super T> actual;

        final Predicate<? super T> predicate;

        QueueSubscription<T> s;

        boolean done;
        
        int sourceMode;

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
                    ExceptionHelper.throwIfFatal(e);
                    s.cancel();
    
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
                    ExceptionHelper.throwIfFatal(e);
                    s.cancel();
    
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
        public boolean isEmpty() {
            return s.isEmpty();
        }

        @Override
        public void clear() {
            s.clear();
        }
        
        @Override
        public int size() {
            return s.size();
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            int m;
            if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
                if ((requestedMode & Fuseable.SYNC) != 0) {
                    m = s.requestFusion(Fuseable.SYNC);
                } else {
                    m = Fuseable.NONE;
                }
            } else {
                m = s.requestFusion(requestedMode);
            }
            sourceMode = m;
            return m;
        }
    }

}
