package reactivestreams.commons;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */
public final class PublisherConcatIterable<T> implements Publisher<T> {
    
    final Iterable<? extends Publisher<? extends T>> iterable;
    
    public PublisherConcatIterable(Iterable<? extends Publisher<? extends T>> iterable) {
        this.iterable = Objects.requireNonNull(iterable, "iterable");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {

        Iterator<? extends Publisher<? extends T>> it;

        try {
            it = iterable.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (it == null) {
            EmptySubscription.error(s, new NullPointerException("The Iterator returned is null"));
            return;
        }
        
        PublisherConcatIterableSubscriber<T> parent = new PublisherConcatIterableSubscriber<>(s, it);
    
        s.onSubscribe(parent.arbiter);
        
        if (!parent.arbiter.isCancelled()) {
            parent.onComplete();
        }
    }
    
    static final class PublisherConcatIterableSubscriber<T> 
    implements Subscriber<T> {

        final Subscriber<? super T> actual;
        
        final Iterator<? extends Publisher<? extends T>> it;
        
        final MultiSubscriptionArbiter arbiter;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatIterableSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherConcatIterableSubscriber.class, "wip");
        
        long produced;
        
        public PublisherConcatIterableSubscriber(Subscriber<? super T> actual, Iterator<? extends Publisher<? extends T>> it) {
            this.actual = actual;
            this.it = it;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            produced++;
            
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (WIP.getAndIncrement(this) == 0) {
                Iterator<? extends Publisher<? extends T>> a = this.it;
                do {
                    if (arbiter.isCancelled()) {
                        return;
                    }
                    
                    boolean b;
                    
                    try {
                        b = a.hasNext();
                    } catch (Throwable e) {
                        actual.onError(e);
                        return;
                    }

                    if (arbiter.isCancelled()) {
                        return;
                    }

                    
                    if (!b) {
                        actual.onComplete();
                        return;
                    }

                    Publisher<? extends T> p;

                    try {
                        p = it.next();
                    } catch (Throwable e) {
                        actual.onError(e);
                        return;
                    }

                    if (arbiter.isCancelled()) {
                        return;
                    }

                    if (p == null) {
                        actual.onError(new NullPointerException("The Publisher returned by the iterator is null"));
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        arbiter.produced(c);
                    }
                    
                    p.subscribe(this);
                    
                    if (arbiter.isCancelled()) {
                        return;
                    }
                    
                } while (WIP.decrementAndGet(this) != 0);
            }
            
        }
    }
}
