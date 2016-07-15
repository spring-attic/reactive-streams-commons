package rsc.publisher;

import java.util.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.flow.Fuseable.ConditionalSubscriber;

import rsc.flow.Trackable;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;
import rsc.flow.Fuseable.*;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL }, output = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL })
public final class PublisherDistinct<T, K, C extends Collection<? super K>> extends PublisherSource<T, T> {

    final Function<? super T, ? extends K> keyExtractor;

    final Supplier<C> collectionSupplier;

    public PublisherDistinct(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
                             Supplier<C> collectionSupplier) {
        super(source);
        this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
        this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        C collection;

        try {
            collection = collectionSupplier.get();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }

        if (collection == null) {
            SubscriptionHelper.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
            return;
        }
        
        if (source instanceof Fuseable) {
            source.subscribe(new PublisherDistinctFuseableSubscriber<>(s, collection, keyExtractor));
        } else
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new PublisherDistinctConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, collection, keyExtractor));
        } else {
            source.subscribe(new PublisherDistinctSubscriber<>(s, collection, keyExtractor));
        }
    }

    static final class PublisherDistinctSubscriber<T, K, C extends Collection<? super K>>
            implements Fuseable.ConditionalSubscriber<T>, Receiver, Producer, Loopback,
                       Subscription, Trackable {
        final Subscriber<? super T> actual;

        final C collection;

        final Function<? super T, ? extends K> keyExtractor;

        Subscription s;

        boolean done;

        public PublisherDistinctSubscriber(Subscriber<? super T> actual, C collection,
                                           Function<? super T, ? extends K> keyExtractor) {
            this.actual = actual;
            this.collection = collection;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t)) {
                s.request(1);
            }
        }
        
        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return true;
            }

            boolean b;

            try {
                b = collection.add(k);
            } catch (Throwable e) {
                s.cancel();

                onError(e);
                return true;
            }


            if (b) {
                actual.onNext(t);
                return true;
            }
            return false;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            collection.clear();

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            collection.clear();

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
            return keyExtractor;
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
    }

    static final class PublisherDistinctConditionalSubscriber<T, K, C extends Collection<? super K>>
    implements Fuseable.ConditionalSubscriber<T>, Receiver, Producer, Loopback,
               Subscription, Trackable {
        final ConditionalSubscriber<? super T> actual;

        final C collection;

        final Function<? super T, ? extends K> keyExtractor;

        Subscription s;

        boolean done;

        public PublisherDistinctConditionalSubscriber(ConditionalSubscriber<? super T> actual, C collection,
                Function<? super T, ? extends K> keyExtractor) {
            this.actual = actual;
            this.collection = collection;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return;
            }

            boolean b;

            try {
                b = collection.add(k);
            } catch (Throwable e) {
                s.cancel();

                onError(e);
                return;
            }


            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return true;
            }

            boolean b;

            try {
                b = collection.add(k);
            } catch (Throwable e) {
                s.cancel();

                onError(e);
                return true;
            }


            if (b) {
                return actual.tryOnNext(t);
            }
            return false;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            collection.clear();

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            collection.clear();

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
            return keyExtractor;
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
    }

    
    static final class PublisherDistinctFuseableSubscriber<T, K, C extends Collection<? super K>>
    implements Fuseable.ConditionalSubscriber<T>, Receiver, Producer, Loopback,
               QueueSubscription<T>, Trackable {
        final Subscriber<? super T> actual;

        final C collection;

        final Function<? super T, ? extends K> keyExtractor;

        QueueSubscription<T> qs;

        boolean done;
        
        int sourceMode;

        public PublisherDistinctFuseableSubscriber(Subscriber<? super T> actual, C collection,
                Function<? super T, ? extends K> keyExtractor) {
            this.actual = actual;
            this.collection = collection;
            this.keyExtractor = keyExtractor;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.qs, s)) {
                this.qs = (QueueSubscription<T>)s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t)) {
                qs.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            if (sourceMode == Fuseable.ASYNC) {
                actual.onNext(null);
                return true;
            }
            
            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                qs.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return true;
            }

            boolean b;

            try {
                b = collection.add(k);
            } catch (Throwable e) {
                qs.cancel();

                onError(e);
                return true;
            }


            if (b) {
                actual.onNext(t);
                return true;
            }
            return false;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            collection.clear();

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            collection.clear();

            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return qs != null && !done;
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
            return keyExtractor;
        }

        @Override
        public Object upstream() {
            return qs;
        }

        @Override
        public void request(long n) {
            qs.request(n);
        }

        @Override
        public void cancel() {
            qs.cancel();
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            int m = qs.requestFusion(requestedMode);
            sourceMode = m;
            return m;
        }
        
        @Override
        public T poll() {
            if (sourceMode == Fuseable.ASYNC) {
                long dropped = 0;
                for (;;) {
                    T v = qs.poll();
    
                    if (v == null || collection.add(keyExtractor.apply(v))) {
                        if (dropped != 0) {
                            request(dropped);
                        }
                        return v;
                    }
                    dropped++;
                }
            } else {
                for (;;) {
                    T v = qs.poll();
    
                    if (v == null || collection.add(keyExtractor.apply(v))) {
                        return v;
                    }
                }
            }
        }
        
        @Override
        public boolean isEmpty() {
            return qs.isEmpty();
        }
        
        @Override
        public void clear() {
            qs.clear();
            collection.clear();
        }
        
        @Override
        public int size() {
            return qs.size();
        }
    }

}
