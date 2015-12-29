package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriber.SerializedSubscriber;
import reactivestreams.commons.internal.subscription.CancelledSubscription;
import reactivestreams.commons.internal.subscription.EmptySubscription;

/**
 * Relays values from the main Publisher until another Publisher signals an event.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 */
public final class PublisherTakeUntil<T, U> implements Publisher<T> {
    
    final Publisher<? extends T> source;
    
    final Publisher<U> other;

    public PublisherTakeUntil(Publisher<? extends T> source, Publisher<U> other) {
        this.source = Objects.requireNonNull(source, "source");
        this.other = Objects.requireNonNull(other, "other");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherTakeUntilMainSubscriber<T> mainSubscriber = new PublisherTakeUntilMainSubscriber<>(s);
        
        PublisherTakeUntilOtherSubscriber<U> otherSubscriber = new PublisherTakeUntilOtherSubscriber<>(mainSubscriber);
        
        other.subscribe(otherSubscriber);
        
        source.subscribe(mainSubscriber);
    }
    
    static final class PublisherTakeUntilOtherSubscriber<U> implements Subscriber<U> {
        final PublisherTakeUntilMainSubscriber<?> main;

        boolean once;
        
        public PublisherTakeUntilOtherSubscriber(PublisherTakeUntilMainSubscriber<?> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            main.setOther(s);
            
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(U t) {
            onComplete();
        }

        @Override
        public void onError(Throwable t) {
            if (once) {
                return;
            }
            once = true;
            main.onError(t);
        }

        @Override
        public void onComplete() {
            if (once) {
                return;
            }
            once = true;
            main.onComplete();
        }

        
    }
    
    static final class PublisherTakeUntilMainSubscriber<T> implements Subscriber<T>, Subscription {
        final SerializedSubscriber<T> actual;
        
        volatile Subscription main;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherTakeUntilMainSubscriber, Subscription> MAIN =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTakeUntilMainSubscriber.class, Subscription.class, "main");
        
        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherTakeUntilMainSubscriber, Subscription> OTHER =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTakeUntilMainSubscriber.class, Subscription.class, "other");
        
        public PublisherTakeUntilMainSubscriber(Subscriber<? super T> actual) {
            this.actual = new SerializedSubscriber<>(actual);
        }
        
        void setOther(Subscription s) {
            if (!OTHER.compareAndSet(this, null, s)) {
                s.cancel();
                if (other != CancelledSubscription.INSTANCE) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            }
        }

        @Override
        public void request(long n) {
            main.request(n);
        }

        void cancelMain() {
            Subscription s = main;
            if (s != CancelledSubscription.INSTANCE) {
                s = MAIN.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }

        void cancelOther() {
            Subscription s = other;
            if (s != CancelledSubscription.INSTANCE) {
                s = OTHER.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }
        
        @Override
        public void cancel() {
            cancelMain();
            cancelOther();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!MAIN.compareAndSet(this, null, s)) {
                s.cancel();
                if (main != CancelledSubscription.INSTANCE) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            } else {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            
            if (main == null) {
                if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
                    EmptySubscription.error(actual, t);
                    return;
                }
            }
            cancel();
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (main == null) {
                if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
                    cancelOther();
                    EmptySubscription.complete(actual);
                    return;
                }
            }
            cancel();
            
            actual.onComplete();
        }
    }
}
