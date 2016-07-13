package rsc.publisher;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.util.BackpressureHelper;
import rsc.subscriber.DeferredSubscription;

import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Buffers elements into custom collections where the buffer boundary is signalled
 * by another publisher.
 *
 * @param <T> the source value type
 * @param <U> the element type of the boundary publisher (irrelevant)
 * @param <C> the output collection type
 */
public final class PublisherBufferBoundary<T, U, C extends Collection<? super T>> 
extends PublisherSource<T, C> {

    final Publisher<U> other;
    
    final Supplier<C> bufferSupplier;

    public PublisherBufferBoundary(Publisher<? extends T> source, 
            Publisher<U> other, Supplier<C> bufferSupplier) {
        super(source);
        this.other = Objects.requireNonNull(other, "other");
        this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
    }

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }
    
    @Override
    public void subscribe(Subscriber<? super C> s) {
        C buffer;
        
        try {
            buffer = bufferSupplier.get();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }
        
        if (buffer == null) {
            SubscriptionHelper.error(s, new NullPointerException("The bufferSupplier returned a null buffer"));
            return;
        }
        
        PublisherBufferBoundaryMain<T, U, C> parent = new PublisherBufferBoundaryMain<>(s, buffer, bufferSupplier);
        
        PublisherBufferBoundaryOther<U> boundary = new PublisherBufferBoundaryOther<>(parent);
        parent.other = boundary;
        
        s.onSubscribe(parent);
        
        other.subscribe(boundary);
        
        source.subscribe(parent);
    }
    
    static final class PublisherBufferBoundaryMain<T, U, C extends Collection<? super T>>
    implements Subscriber<T>, Subscription {

        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        PublisherBufferBoundaryOther<U> other;
        
        C buffer;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherBufferBoundaryMain, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherBufferBoundaryMain.class, Subscription.class, "s");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherBufferBoundaryMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherBufferBoundaryMain.class, "requested");
        
        public PublisherBufferBoundaryMain(Subscriber<? super C> actual, C buffer, Supplier<C> bufferSupplier) {
            this.actual = actual;
            this.buffer = buffer;
            this.bufferSupplier = bufferSupplier;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
            }
        }

        void cancelMain() {
            SubscriptionHelper.terminate(S, this);
        }
        
        @Override
        public void cancel() {
            cancelMain();
            other.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            synchronized (this) {
                C b = buffer;
                if (b != null) {
                    b.add(t);
                    return;
                }
            }
            
            UnsignalledExceptions.onNextDropped(t);
        }

        @Override
        public void onError(Throwable t) {
            boolean report;
            synchronized (this) {
                C b = buffer;
                
                if (b != null) {
                    buffer = null;
                    report = true;
                } else {
                    report = false;
                }
            }
            
            if (report) {
                other.cancel();
                
                actual.onError(t);
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }

        @Override
        public void onComplete() {
            C b;
            synchronized (this) {
                b = buffer;
                buffer = null;
            }
            
            if (b != null && !b.isEmpty()) {
                if (emit(b)) {
                    actual.onComplete();
                }
            }
        }
        
        void otherNext() {
            C c;
            
            try {
                c = bufferSupplier.get();
            } catch (Throwable e) {
                other.cancel();
                
                otherError(e);
                return;
            }
            
            if (c == null) {
                other.cancel();

                otherError(new NullPointerException("The bufferSupplier returned a null buffer"));
                return;
            }
            
            C b;
            synchronized (this) {
                b = buffer;
                if (b == null) {
                    return;
                }
                buffer = c;
            }
            
            emit(b);
        }
        
        void otherError(Throwable e) {
            cancelMain();
            
            onError(e);
        }
        
        void otherComplete() {
            cancelMain();

            onComplete();
        }
        
        boolean emit(C b) {
            long r = requested;
            if (r != 0L) {
                actual.onNext(b);
                if (r != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }
                return true;
            } else {
                cancel();
                
                actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));

                return false;
            }
        }
    }
    
    static final class PublisherBufferBoundaryOther<U> extends DeferredSubscription
    implements Subscriber<U> {
        
        final PublisherBufferBoundaryMain<?, U, ?> main;
        
        public PublisherBufferBoundaryOther(PublisherBufferBoundaryMain<?, U, ?> main) {
            this.main = main;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(U t) {
            main.otherNext();
        }
        
        @Override
        public void onError(Throwable t) {
            main.otherError(t);
        }
        
        @Override
        public void onComplete() {
            main.otherComplete();
        }
    }
}
