package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.reactivestreams.*;

import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.subscriber.SubscriberDeferSubscriptionBase;
import reactivestreams.commons.support.*;

/**
 * buffers elements into possibly overlapping buffers whose boundaries are determined
  by a start Publisher's element and a signal of a derived Publisher
 *
 * @param <T> the source value type
 * @param <U> the value type of the publisher opening the buffers
 * @param <V> the value type of the publisher closing the individual buffers
 * @param <C> the collection type that holds the buffered values
 */
public final class PublisherBufferStartEnd<T, U, V, C extends Collection<? super T>>
extends PublisherSource<T, C> {

    final Publisher<U> start;
    
    final Function<? super U, ? extends Publisher<V>> end;
    
    final Supplier<C> bufferSupplier;

    public PublisherBufferStartEnd(Publisher<? extends T> source, Publisher<U> start,
            Function<? super U, ? extends Publisher<V>> end, Supplier<C> bufferSupplier) {
        super(source);
        this.start = start;
        this.end = end;
        this.bufferSupplier = bufferSupplier;
    }
    
    @Override
    public void subscribe(Subscriber<? super C> s) {
        PublisherBufferStartEndMain<T, U, V, C> parent = new PublisherBufferStartEndMain<>(s, bufferSupplier);
        
        PublisherBufferStartEndStarter<U> starter = new PublisherBufferStartEndStarter<>(parent);
        parent.starter = starter;
        
        s.onSubscribe(parent);
        
        start.subscribe(starter);
        
        source.subscribe(parent);
    }
    
    static final class PublisherBufferStartEndMain<T, U, V, C extends Collection<? super T>>
    implements Subscriber<T>, Subscription {
        
        final Subscriber<? super C> actual;
        
        final Supplier<C> bufferSupplier;
        
        PublisherBufferStartEndStarter<U> starter;

        Collection<C> buffers;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherBufferStartEndMain, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherBufferStartEndMain.class, Subscription.class, "s");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherBufferStartEndMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherBufferStartEndMain.class, "requested");

        public PublisherBufferStartEndMain(Subscriber<? super C> actual, Supplier<C> bufferSupplier) {
            this.actual = actual;
            this.bufferSupplier = bufferSupplier;
            this.buffers = new HashSet<>();
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
                Collection<C> set = buffers;
                if (set != null) {
                    for (C b : set) {
                        b.add(t);
                    }
                    return;
                }
            }
            
            UnsignalledExceptions.onNextDropped(t);
        }
        
        @Override
        public void onError(Throwable t) {
            boolean report;
            synchronized (this) {
                Collection<C> set = buffers;
                if (set != null) {
                    buffers = null;
                    report = true;
                } else {
                    report = false;
                }
            }
            
            if (report) {
                actual.onError(t);
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onComplete() {
            Collection<C> set;
            
            synchronized (this) {
                set = buffers;
                if (set == null) {
                    return;
                }
            }
            
            for (C b : set) {
                if (!emit(b)) {
                    break;
                }
            }
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
            }
        }
        
        void cancelMain() {
            SubscriptionHelper.terminate(S, this);
        }
        
        void cancelStart() {
            starter.cancel();
        }
        
        @Override
        public void cancel() {
            // TODO Auto-generated method stub
            
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
                
                actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));;
                
                return false;
            }
        }
    }
    
    static final class PublisherBufferStartEndStarter<U> extends SubscriberDeferSubscriptionBase
    implements Subscriber<U> {
        final PublisherBufferStartEndMain<?, U, ?, ?> main;
        
        public PublisherBufferStartEndStarter(PublisherBufferStartEndMain<?, U, ?, ?> main) {
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
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void onError(Throwable t) {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void onComplete() {
            // TODO Auto-generated method stub
            
        }
    }}
