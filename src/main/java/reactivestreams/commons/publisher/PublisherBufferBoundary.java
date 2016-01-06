package reactivestreams.commons.publisher;

import java.util.Collection;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactivestreams.commons.subscriber.SubscriberDeferSubscriptionBase;
import reactivestreams.commons.subscription.EmptySubscription;

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
        this.other = other;
        this.bufferSupplier = bufferSupplier;
    }
    
    @Override
    public void subscribe(Subscriber<? super C> s) {
        C buffer;
        
        try {
            buffer = bufferSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (buffer == null) {
            EmptySubscription.error(s, new NullPointerException("The bufferSupplier returned a null buffer"));
            return;
        }
        
        PublisherBufferBoundaryMain<T, U, C> parent = new PublisherBufferBoundaryMain<>(s, buffer);
        
        PublisherBufferBoundaryOther<U> boundary = new PublisherBufferBoundaryOther<>(parent);
        parent.other = boundary;
        
        s.onSubscribe(parent);
        
        other.subscribe(boundary);
        
        source.subscribe(parent);
    }
    
    static final class PublisherBufferBoundaryMain<T, U, C extends Collection<? super T>>
    implements Subscriber<T>, Subscription {

        final Subscriber<? super C> actual;
        
        PublisherBufferBoundaryOther<U> other;
        
        C buffer;
        
        Subscription s;
        
        public PublisherBufferBoundaryMain(Subscriber<? super C> actual, C buffer) {
            this.actual = actual;
            this.buffer = buffer;
        }
        
        @Override
        public void request(long n) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onSubscribe(Subscription s) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onNext(T t) {
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
        
        void otherNext() {
            
        }
        
        void otherError(Throwable e) {
            
        }
        
        void otherComplete() {
            
        }
    }
    
    static final class PublisherBufferBoundaryOther<U> extends SubscriberDeferSubscriptionBase
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
