package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.reactivestreams.Subscriber;

import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class PublisherSubscribeOnValue<T> extends PublisherBase<T> {

    final T value;
    
    final Callable<Function<Runnable, Runnable>> schedulerFactory;

    final boolean eagerCancel;

    public PublisherSubscribeOnValue(T value, 
            Callable<Function<Runnable, Runnable>> schedulerFactory, 
            boolean eagerCancel) {
        this.value = value;
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
        this.eagerCancel = eagerCancel;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Function<Runnable, Runnable> scheduler;
        
        try {
            scheduler = schedulerFactory.call();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            EmptySubscription.error(s, e);
            return;
        }
        
        if (scheduler == null) {
            EmptySubscription.error(s, new NullPointerException("The schedulerFactory returned a null Function"));
            return;
        }

        PublisherSubscribeOnOther.supplierScheduleOnSubscribe(value, s, scheduler, eagerCancel);
    }
}
