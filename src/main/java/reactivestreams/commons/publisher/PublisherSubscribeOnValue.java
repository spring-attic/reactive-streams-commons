package reactivestreams.commons.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import reactivestreams.commons.flow.Cancellation;
import reactivestreams.commons.publisher.PublisherSubscribeOn.*;
import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.scheduler.Scheduler.Worker;
import reactivestreams.commons.util.*;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class PublisherSubscribeOnValue<T> extends Px<T> {

    final T value;
    
    final Scheduler scheduler;

    final boolean eagerCancel;

    public PublisherSubscribeOnValue(T value, 
            Scheduler scheduler, 
            boolean eagerCancel) {
        this.value = value;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.eagerCancel = eagerCancel;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Worker worker;
        
        try {
            worker = scheduler.createWorker();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            EmptySubscription.error(s, e);
            return;
        }
        
        if (worker == null) {
            EmptySubscription.error(s, new NullPointerException("The scheduler returned a null Function"));
            return;
        }

        T v = value;
        if (v == null) {
            ScheduledEmpty parent = new ScheduledEmpty(s);
            s.onSubscribe(parent);
            Cancellation f = scheduler.schedule(parent);
            parent.setFuture(f);
        } else {
            s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
        }
    }
}
