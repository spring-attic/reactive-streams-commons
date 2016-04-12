package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.*;

import rsc.scheduler.TimedScheduler;
import rsc.scheduler.TimedScheduler.TimedWorker;
import rsc.util.BackpressureHelper;
import rsc.util.SubscriptionHelper;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 */
public final class PublisherInterval extends Px<Long> {

    final TimedScheduler timedScheduler;
    
    final long initialDelay;
    
    final long period;
    
    final TimeUnit unit;

    public PublisherInterval(
            long initialDelay, 
            long period, 
            TimeUnit unit, 
            TimedScheduler timedScheduler) {
        if (period < 0L) {
            throw new IllegalArgumentException("period >= 0 required but it was " + period);
        }
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = Objects.requireNonNull(unit, "unit");
        this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        
        TimedWorker w = timedScheduler.createWorker();
        
        PublisherIntervalRunnable r = new PublisherIntervalRunnable(s, w);
        
        s.onSubscribe(r);

        w.schedulePeriodically(r, initialDelay, period, unit);
    }
    
    static final class PublisherIntervalRunnable implements Runnable, Subscription {
        final Subscriber<? super Long> s;
        
        final TimedWorker worker;
        
        volatile long requested;
        static final AtomicLongFieldUpdater<PublisherIntervalRunnable> REQUESTED = 
                AtomicLongFieldUpdater.newUpdater(PublisherIntervalRunnable.class, "requested");
        
        long count;
        
        volatile boolean cancelled;

        public PublisherIntervalRunnable(Subscriber<? super Long> s, TimedWorker worker) {
            this.s = s;
            this.worker = worker;
        }
        
        @Override
        public void run() {
            if (!cancelled) {
                if (requested != 0L) {
                    s.onNext(count++);
                    if (requested != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                } else {
                    cancel();
                    
                    s.onError(new IllegalStateException("Could not emit value " + count + " due to lack of requests"));
                }
            }
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
            }
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                worker.shutdown();
            }
        }
    }
}
