package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import rsc.flow.Cancellation;
import rsc.scheduler.Scheduler;
import rsc.util.EmptySubscription;
import rsc.util.ExceptionHelper;

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
        Scheduler.Worker worker;
        
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
            PublisherSubscribeOn.ScheduledEmpty parent = new PublisherSubscribeOn.ScheduledEmpty(s);
            s.onSubscribe(parent);
            Cancellation f = scheduler.schedule(parent);
            parent.setFuture(f);
        } else {
            s.onSubscribe(new PublisherSubscribeOn.ScheduledScalar<>(s, v, scheduler));
        }
    }
}
