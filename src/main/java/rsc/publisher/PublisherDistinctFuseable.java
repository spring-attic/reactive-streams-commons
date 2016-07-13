package rsc.publisher;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.flow.Fuseable;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.publisher.PublisherDistinct.PublisherDistinctFuseableSubscriber;
import rsc.subscriber.SubscriptionHelper;

/**
 * For each subscriber, tracks the source values that have been seen and
 * filters out duplicates.
 *
 * @param <T> the source value type
 * @param <K> the key extacted from the source value to be used for duplicate testing
 * @param <C> the collection type whose add() method is used for testing for duplicates
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL }, output = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL })
public final class PublisherDistinctFuseable<T, K, C extends Collection<? super K>> 
extends PublisherSource<T, T> implements Fuseable {

    final Function<? super T, ? extends K> keyExtractor;

    final Supplier<C> collectionSupplier;

    public PublisherDistinctFuseable(Publisher<? extends T> source, Function<? super T, ? extends K> keyExtractor,
                             Supplier<C> collectionSupplier) {
        super(source);
        this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
        this.collectionSupplier = Objects.requireNonNull(collectionSupplier, "collectionSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        C collection;

        try {
            collection = collectionSupplier.get();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }

        if (collection == null) {
            SubscriptionHelper.error(s, new NullPointerException("The collectionSupplier returned a null collection"));
            return;
        }
        
        source.subscribe(new PublisherDistinctFuseableSubscriber<>(s, collection, keyExtractor));
    }
}
