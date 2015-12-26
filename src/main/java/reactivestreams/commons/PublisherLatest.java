package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.BackpressureHelper;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Runs the source in unbounded mode and emits only the latest value
 * if the subscriber can't keep up properly.
 *
 * @param <T> the value type
 */
public final class PublisherLatest<T> implements Publisher<T> {

    final Publisher<? extends T> source;

    public PublisherLatest(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherLatestSubscriber<>(s));
    }

    static final class PublisherLatestSubscriber<T> implements Subscriber<T>, Subscription {

        final Subscriber<? super T> actual;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherLatestSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherLatestSubscriber.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherLatestSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherLatestSubscriber.class, "wip");

        Subscription s;
        
        Throwable error;
        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile T value;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherLatestSubscriber, Object> VALUE =
                AtomicReferenceFieldUpdater.newUpdater(PublisherLatestSubscriber.class, Object.class, "value");
        
        public PublisherLatestSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(REQUESTED, this, n);
                
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                
                cancelled = true;
                
                s.cancel();

                if (WIP.getAndIncrement(this) == 0) {
                    VALUE.lazySet(this, null);
                }
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            VALUE.lazySet(this, t);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            final Subscriber<? super T> a = actual;
            
            int missed = 1;
            
            for (;;) {
                
                if (checkTerminated(done, value == null, a)) {
                    return;
                }
                
                long r = requested;
                
                while (r != 0L) {
                    boolean d = done;
                    
                    @SuppressWarnings("unchecked")
                    T v = (T)VALUE.getAndSet(this, null);
                    
                    boolean empty = v == null;
                    
                    if (checkTerminated(d, empty, a)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    a.onNext(v);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                }

                if (checkTerminated(done, value == null, a)) {
                    return;
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
            if (cancelled) {
                VALUE.lazySet(this, null);
                return true;
            }
            
            if (d) {
                Throwable e = error;
                if (e != null) {
                    VALUE.lazySet(this, null);

                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    a.onComplete();
                    return true;
                }
            }
            
            return false;
        }
    }
}
