package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;
import reactivestreams.commons.internal.SingleSubscriptionArbiter;
import reactivestreams.commons.internal.TestProcessor;
import reactivestreams.commons.internal.subscribers.SerializedSubscriber;

/**
 * retries a source when a companion sequence signals 
 * an item in response to the main's error signal
 *
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 * 
 * @param <T> the source value type
 */
public final class PublisherRetryWhen<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Function<Publisher<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory;

    public PublisherRetryWhen(Publisher<? extends T> source,
            Function<Publisher<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory) {
        this.source = Objects.requireNonNull(source, "source");
        this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRetryWhenOtherSubscriber other = new PublisherRetryWhenOtherSubscriber();

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);
        
        PublisherRetryWhenMainSubscriber<T> main = new PublisherRetryWhenMainSubscriber<>(serial, other.completionSignal, source);
        other.main = main;

        serial.onSubscribe(main.arbiter);

        Publisher<? extends Object> p;
        
        try {
            p = whenSourceFactory.apply(other);
        } catch (Throwable e) {
            s.onError(e);
            return;
        }
        
        if (p == null) {
            s.onError(new NullPointerException("The whenSourceFactory returned a null Publisher"));
            return;
        }
        
        p.subscribe(other);

        if (!main.cancelled) {
            source.subscribe(main);
        }
    }
    
    static final class PublisherRetryWhenMainSubscriber<T> implements Subscriber<T>, Subscription {
        
        final Subscriber<? super T> actual;
        
        final MultiSubscriptionArbiter arbiter;

        final SingleSubscriptionArbiter otherArbiter;
        
        final Subscriber<Throwable> signaller;
        
        final Publisher<? extends T> source;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRetryWhenMainSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherRetryWhenMainSubscriber.class, "wip");

        volatile boolean cancelled;
        
        static final Object NEXT = new Object();
        
        public PublisherRetryWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Throwable> signaller, Publisher<? extends T> source) {
            this.actual = actual;
            this.signaller = signaller;
            this.source = source;
            this.arbiter = new MultiSubscriptionArbiter();
            this.otherArbiter = new SingleSubscriptionArbiter();
        }

        @Override
        public void request(long n) {
            arbiter.request(n);
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            
            cancelWhen();
            
            arbiter.cancel();
        }

        void cancelWhen() {
            otherArbiter.cancel();
        }
        
        public void setWhen(Subscription w) {
            otherArbiter.set(w);
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
            
            arbiter.producedOne();
        }

        @Override
        public void onError(Throwable t) {
            otherArbiter.request(1);
            
            signaller.onNext(t);
        }

        @Override
        public void onComplete() {
            otherArbiter.cancel();
            
            actual.onComplete();
        }
    
        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (cancelled) {
                        return;
                    }
                    
                    source.subscribe(this);
                    
                } while (WIP.decrementAndGet(this) != 0);
            }
        }
        
        void whenError(Throwable e) {
            cancelled = true;
            arbiter.cancel();
            
            actual.onError(e);
        }
        
        void whenComplete() {
            cancelled = true;
            arbiter.cancel();
            
            actual.onComplete();
        }
    }
    
    static final class PublisherRetryWhenOtherSubscriber implements Subscriber<Object>, Publisher<Throwable> {
        PublisherRetryWhenMainSubscriber<?> main;

        final TestProcessor<Throwable> completionSignal = new TestProcessor<>();
        
        @Override
        public void onSubscribe(Subscription s) {
            main.setWhen(s);
        }

        @Override
        public void onNext(Object t) {
            main.resubscribe();
        }

        @Override
        public void onError(Throwable t) {
            main.whenError(t);
        }

        @Override
        public void onComplete() {
            main.whenComplete();
        }
        
        @Override
        public void subscribe(Subscriber<? super Throwable> s) {
            completionSignal.subscribe(s);
        }
    }
}
