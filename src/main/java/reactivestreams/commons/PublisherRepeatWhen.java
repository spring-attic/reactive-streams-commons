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
 * Repeats a source when a companion sequence 
 * signals an item in response to the main's completion signal
 *
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 * 
 * @param <T> the source value type
 */
public final class PublisherRepeatWhen<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Function<Publisher<Object>, ? extends Publisher<? extends Object>> whenSourceFactory;

    public PublisherRepeatWhen(Publisher<? extends T> source,
            Function<Publisher<Object>, ? extends Publisher<? extends Object>> whenSourceFactory) {
        this.source = Objects.requireNonNull(source, "source");
        this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRepeatWhenOtherSubscriber other = new PublisherRepeatWhenOtherSubscriber();

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);
        
        PublisherRepeatWhenMainSubscriber<T> main = new PublisherRepeatWhenMainSubscriber<>(serial, other.completionSignal, source);
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
    
    static final class PublisherRepeatWhenMainSubscriber<T> implements Subscriber<T>, Subscription {
        
        final Subscriber<? super T> actual;
        
        final MultiSubscriptionArbiter arbiter;

        final SingleSubscriptionArbiter otherArbiter;
        
        final Subscriber<Object> signaller;
        
        final Publisher<? extends T> source;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRepeatWhenMainSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherRepeatWhenMainSubscriber.class, "wip");

        volatile boolean cancelled;
        
        static final Object NEXT = new Object();
        
        public PublisherRepeatWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Object> signaller, Publisher<? extends T> source) {
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
            otherArbiter.cancel();
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            otherArbiter.request(1);
            
            signaller.onNext(NEXT);
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
    
    static final class PublisherRepeatWhenOtherSubscriber implements Subscriber<Object>, Publisher<Object> {
        PublisherRepeatWhenMainSubscriber<?> main;

        final TestProcessor<Object> completionSignal = new TestProcessor<>();
        
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
        public void subscribe(Subscriber<? super Object> s) {
            completionSignal.subscribe(s);
        }
    }
}
