package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.*;

import reactivestreams.commons.scheduler.TimedScheduler;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.util.*;

/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 */
public final class PublisherTimer extends Px<Long> {

    final TimedScheduler timedScheduler;
    
    final long delay;
    
    final TimeUnit unit;
    
    public PublisherTimer(long delay, TimeUnit unit, TimedScheduler timedScheduler) {
        this.delay = delay;
        this.unit = Objects.requireNonNull(unit, "unit");
        this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        PublisherTimerRunnable r = new PublisherTimerRunnable(s);
        
        s.onSubscribe(r);
        
        r.setCancel(timedScheduler.schedule(r, delay, unit));
    }
    
    static final class PublisherTimerRunnable implements Runnable, Subscription {
        final Subscriber<? super Long> s;
        
        volatile Cancellable cancel;
        static final AtomicReferenceFieldUpdater<PublisherTimerRunnable, Cancellable> CANCEL =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTimerRunnable.class, Cancellable.class, "cancel");
        
        volatile boolean requested;

        public PublisherTimerRunnable(Subscriber<? super Long> s) {
            this.s = s;
        }
        
        public void setCancel(Cancellable cancel) {
            if (!CANCEL.compareAndSet(this, null, cancel)) {
                cancel.cancel();
            }
        }
        
        @Override
        public void run() {
            if (requested) {
                if (cancel != Cancellable.CANCELLED) {
                    s.onNext(0L);
                }
                if (cancel != Cancellable.CANCELLED) {
                    s.onComplete();
                }
            } else {
                s.onError(new IllegalStateException("Could not emit value due to lack of requests"));
            }
        }
        
        @Override
        public void cancel() {
            Cancellable c = cancel;
            if (c != Cancellable.CANCELLED) {
                c =  CANCEL.getAndSet(this, Cancellable.CANCELLED);
                if (c != null && c != Cancellable.CANCELLED) {
                    c.cancel();
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
