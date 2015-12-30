package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscriber.SerializedSubscriber;
import reactivestreams.commons.internal.subscription.CancelledSubscription;
import reactivestreams.commons.internal.subscription.EmptySubscription;
import reactivestreams.commons.internal.support.SubscriptionHelper;

/**
 * Skips values from the main publisher until the other publisher signals
 * an onNext or onComplete.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 */
public final class PublisherSkipUntil<T, U> extends PublisherSource<T, T> {
    
    final Publisher<U> other;

    public PublisherSkipUntil(Publisher<? extends T> source, Publisher<U> other) {
        super(source);
        this.other = Objects.requireNonNull(other, "other");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherSkipUntilMainSubscriber<T> mainSubscriber = new PublisherSkipUntilMainSubscriber<>(s);
        
        PublisherSkipUntilOtherSubscriber<U> otherSubscriber = new PublisherSkipUntilOtherSubscriber<>(mainSubscriber);
        
        other.subscribe(otherSubscriber);
        
        source.subscribe(mainSubscriber);
    }
    
    static final class PublisherSkipUntilOtherSubscriber<U> implements Subscriber<U> {
        final PublisherSkipUntilMainSubscriber<?> main;

        public PublisherSkipUntilOtherSubscriber(PublisherSkipUntilMainSubscriber<?> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            main.setOther(s);
            
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(U t) {
            if (main.gate) {
                return;
            }
            PublisherSkipUntilMainSubscriber<?> m = main;
            m.other.cancel();
            m.gate = true;
            m.other = CancelledSubscription.INSTANCE;
        }

        @Override
        public void onError(Throwable t) {
            PublisherSkipUntilMainSubscriber<?> m = main;
            if (m.gate) {
                return;
            }
            m.onError(t);
        }

        @Override
        public void onComplete() {
            PublisherSkipUntilMainSubscriber<?> m = main;
            if (m.gate) {
                return;
            }
            m.gate = true;
            m.other = CancelledSubscription.INSTANCE;
        }

        
    }
    
    static final class PublisherSkipUntilMainSubscriber<T> 
    implements Subscriber<T>, Subscription {

        final SerializedSubscriber<T> actual;
        
        volatile Subscription main;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherSkipUntilMainSubscriber, Subscription> MAIN =
                AtomicReferenceFieldUpdater.newUpdater(PublisherSkipUntilMainSubscriber.class, Subscription.class, "main");
        
        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherSkipUntilMainSubscriber, Subscription> OTHER =
                AtomicReferenceFieldUpdater.newUpdater(PublisherSkipUntilMainSubscriber.class, Subscription.class, "other");

        volatile boolean gate;

        public PublisherSkipUntilMainSubscriber(Subscriber<? super T> actual) {
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
            if (gate) {
                actual.onNext(t);
            } else {
                main.request(1);
            }
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
            cancelOther();
            
            actual.onComplete();
        }
    }
}
