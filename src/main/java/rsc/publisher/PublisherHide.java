package rsc.publisher;

import org.reactivestreams.*;

/**
 * Hides the identities of the upstream Publisher object and its Subscription
 * as well. 
 *
 * @param <T> the value type
 */
public final class PublisherHide<T> extends PublisherSource<T, T> {

    public PublisherHide(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherHideSubscriber<>(s));
    }

    static final class PublisherHideSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;

        Subscription s;
        
        public PublisherHideSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
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
        public void onSubscribe(Subscription s) {
            this.s = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
}
