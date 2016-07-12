package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.*;

import rsc.flow.Cancellation;
import rsc.scheduler.TimedScheduler;
import rsc.subscriber.SubscriptionHelper;

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
        
        volatile Cancellation cancel;
        static final AtomicReferenceFieldUpdater<PublisherTimerRunnable, Cancellation> CANCEL =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTimerRunnable.class, Cancellation.class, "cancel");
        
        volatile boolean requested;
        
        static final Cancellation CANCELLED = () -> { };

        public PublisherTimerRunnable(Subscriber<? super Long> s) {
            this.s = s;
        }
        
        public void setCancel(Cancellation cancel) {
            if (!CANCEL.compareAndSet(this, null, cancel)) {
                cancel.dispose();
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
            Cancellation c = cancel;
            if (c != CANCELLED) {
                c =  CANCEL.getAndSet(this, CANCELLED);
                if (c != null && c != CANCELLED) {
                    c.dispose();
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
