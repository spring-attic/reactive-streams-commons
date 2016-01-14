package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.*;

import reactivestreams.commons.support.SubscriptionHelper;

/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 */
public final class PublisherTimer extends PublisherBase<Long> {

    final Function<Runnable, ? extends Runnable> asyncExecutor;
    
    public PublisherTimer(long delay, TimeUnit unit, ScheduledExecutorService executor) {
        Objects.requireNonNull(unit, "unit");
        Objects.requireNonNull(executor, "executor");
        asyncExecutor = r -> {
            Future<?> f = executor.schedule(r, delay, unit);
            return () -> f.cancel(true);
        };
    }
    
    public PublisherTimer(Function<Runnable, ? extends Runnable> asyncExecutor) {
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor, "asyncExecutor");
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        PublisherTimerRunnable r = new PublisherTimerRunnable(s);
        
        s.onSubscribe(r);
        
        r.setCancel(asyncExecutor.apply(r));
    }
    
    static final class PublisherTimerRunnable implements Runnable, Subscription {
        final Subscriber<? super Long> s;
        
        volatile Runnable cancel;
        static final AtomicReferenceFieldUpdater<PublisherTimerRunnable, Runnable> CANCEL =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTimerRunnable.class, Runnable.class, "cancel");
        
        static final Runnable CANCELLED = () -> { };
        
        volatile boolean requested;

        public PublisherTimerRunnable(Subscriber<? super Long> s) {
            this.s = s;
        }
        
        public void setCancel(Runnable cancel) {
            if (!CANCEL.compareAndSet(this, null, cancel)) {
                cancel.run();
            }
        }
        
        @Override
        public void run() {
            if (requested) {
                if (cancel != CANCELLED) {
                    s.onNext(0L);
                }
                if (cancel != CANCELLED) {
                    s.onComplete();
                }
            } else {
                s.onError(new IllegalStateException("Could not emit value due to lack of requests"));
            }
        }
        
        @Override
        public void cancel() {
            Runnable c = cancel;
            if (c != CANCELLED) {
                c =  CANCEL.getAndSet(this, CANCELLED);
                if (c != null && c != CANCELLED) {
                    c.run();
                }
            }
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                requested = true;
            }
        }
    }
}
