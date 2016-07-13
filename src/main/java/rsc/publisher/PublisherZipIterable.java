package rsc.publisher;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.flow.MultiReceiver;
import rsc.flow.Producer;
import rsc.flow.Receiver;
import rsc.subscriber.SubscriberState;

import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Pairwise combines elements of a publisher and an iterable sequence through a function.
 *
 * @param <T> the main source value type
 * @param <U> the iterable source value type
 * @param <R> the result type
 */
public final class PublisherZipIterable<T, U, R> extends PublisherSource<T, R> {

    final Iterable<? extends U> other;
    
    final BiFunction<? super T, ? super U, ? extends R> zipper;

    public PublisherZipIterable(
            Publisher<? extends T> source, 
            Iterable<? extends U> other,
            BiFunction<? super T, ? super U, ? extends R> zipper) {
        super(source);
        this.other = Objects.requireNonNull(other, "other");
        this.zipper = Objects.requireNonNull(zipper, "zipper");
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        Iterator<? extends U> it;
        
        try {
            it = other.iterator();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }
        
        if (it == null) {
            SubscriptionHelper.error(s, new NullPointerException("The other iterable produced a null iterator"));
            return;
        }
        
        boolean b;
        
        try {
            b = it.hasNext();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }
        
        if (!b) {
            SubscriptionHelper.complete(s);
            return;
        }
        
        source.subscribe(new PublisherZipSubscriber<>(s, it, zipper));
    }
    
    static final class PublisherZipSubscriber<T, U, R> implements Subscriber<T>, Producer, MultiReceiver,
                                                                  Receiver, Subscription,
                                                                  SubscriberState {
        
        final Subscriber<? super R> actual;
        
        final Iterator<? extends U> it;
        
        final BiFunction<? super T, ? super U, ? extends R> zipper;
        
        Subscription s;
        
        boolean done;

        public PublisherZipSubscriber(Subscriber<? super R> actual, Iterator<? extends U> it,
                BiFunction<? super T, ? super U, ? extends R> zipper) {
            this.actual = actual;
            this.it = it;
            this.zipper = zipper;
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

            U u;
            
            try {
                u = it.next();
            } catch (Throwable e) {
                done = true;
                s.cancel();
                
                actual.onError(e);
                return;
            }
            
            R r;
            
            try {
                r = zipper.apply(t, u);
            } catch (Throwable e) {
                done = true;
                s.cancel();
                
                actual.onError(e);
                return;
            }

            
            if (r == null) {
                done = true;
                s.cancel();
                
                actual.onError(new NullPointerException("The zipper returned a null value"));
                return;
            }
            
            actual.onNext(r);
            
            boolean b;
            
            try {
                b = it.hasNext();
            } catch (Throwable e) {
                done = true;
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                actual.onError(ExceptionHelper.unwrap(e));
                return;
            }
            
            if (!b) {
                done = true;
                s.cancel();
                actual.onComplete();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
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
        public Object upstream() {
            return s;
        }

        @Override
        public Iterator<?> upstreams() {
            return isStarted() ? Arrays.asList(s, it).iterator() : null;
        }

        @Override
        public long upstreamCount() {
            return isStarted() ? 2 : 1;
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
}
