package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 */
public final class PublisherResume<T> implements Publisher<T> {
    
    final Publisher<? extends T> source;
    
    final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

    public PublisherResume(Publisher<? extends T> source,
            Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
        this.source = Objects.requireNonNull(source, "source");
        this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherResumeSubscriber<>(s, nextFactory));
    }
    
    static final class PublisherResumeSubscriber<T> implements Subscriber<T> {

        final Subscriber<? super T> actual;
        
        final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

        final MultiSubscriptionArbiter arbiter;
        
        boolean second;

        public PublisherResumeSubscriber(Subscriber<? super T> actual,
                Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
            this.actual = actual;
            this.nextFactory = nextFactory;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!second) {
                actual.onSubscribe(arbiter);
            }
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
            
            if (!second) {
                arbiter.producedOne();
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!second) {
                second = true;
                
                Publisher<? extends T> p;

                try {
                    p = nextFactory.apply(t);
                } catch (Throwable e) {
                    e.addSuppressed(t);
                    actual.onError(e);
                    return;
                }
                if (p == null) {
                    NullPointerException t2 = new NullPointerException("The nextFactory returned a null Publisher");
                    t2.addSuppressed(t);
                    actual.onError(t2);
                } else {
                    p.subscribe(this);
                }
            } else {
                actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
}
