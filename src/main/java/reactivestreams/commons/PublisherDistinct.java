package reactivestreams.commons;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.support.SubscriptionHelper;
import reactivestreams.commons.internal.subscription.EmptySubscription;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 */
public final class PublisherDistinct<T, K, C extends Collection<? super K>> extends PublisherSource<T, T> {

    final Function<? super T, ? extends K> keyExtractor;
    
    final Supplier<C> collectionSupplier;

    public PublisherDistinct(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
            Supplier<C> collectionSuppplier) {
        super(source);
        this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
        this.collectionSupplier = Objects.requireNonNull(collectionSuppplier, "collectionSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        C collection;
        
        try {
            collection = collectionSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (collection == null) {
            EmptySubscription.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
            return;
        }
        
        source.subscribe(new PublisherDistinctSubscriber<>(s, collection, keyExtractor));
    }
    
    static final class PublisherDistinctSubscriber<T, K, C extends Collection<? super K>> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final C collection;
        
        final Function<? super T, ? extends K> keyExtractor;
        
        Subscription s;
        
        boolean done;

        public PublisherDistinctSubscriber(Subscriber<? super T> actual, C collection,
                Function<? super T, ? extends K> keyExtractor) {
            this.actual = actual;
            this.collection = collection;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(s);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            
            K k;
            
            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                
                onError(e);
                return;
            }
            
            boolean b;
            
            try {
                b = collection.add(k);
            } catch (Throwable e) {
                s.cancel();
                
                onError(e);
                return;
            }

            
            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            actual.onComplete();
        }
        
        
    }
}
