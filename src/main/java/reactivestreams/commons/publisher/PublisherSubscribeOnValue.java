package reactivestreams.commons.publisher;

import java.util.function.Function;

import org.reactivestreams.Subscriber;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class PublisherSubscribeOnValue<T> extends PublisherBase<T> {

    final T value;
    
    final Function<Runnable, Runnable> scheduler;

    final boolean eagerCancel;

    public PublisherSubscribeOnValue(T value, Function<Runnable, Runnable> scheduler, boolean eagerCancel) {
        this.value = value;
        this.scheduler = scheduler;
        this.eagerCancel = eagerCancel;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherSubscribeOnOther.supplierScheduleOnSubscribe(value, s, scheduler, eagerCancel);
    }
}
