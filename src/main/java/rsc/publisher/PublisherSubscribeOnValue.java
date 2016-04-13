package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import rsc.flow.Cancellation;
import rsc.scheduler.Scheduler;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class PublisherSubscribeOnValue<T> extends Px<T> {

    final T value;
    
    final Scheduler scheduler;

    public PublisherSubscribeOnValue(T value, 
            Scheduler scheduler) {
        this.value = value;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        T v = value;
        if (v == null) {
            PublisherSubscribeOn.ScheduledEmpty parent = new PublisherSubscribeOn.ScheduledEmpty(s);
            s.onSubscribe(parent);
            Cancellation f = scheduler.schedule(parent);
            parent.setFuture(f);
        } else {
            s.onSubscribe(new PublisherSubscribeOn.ScheduledScalar<>(s, v, scheduler));
        }
    }
}
