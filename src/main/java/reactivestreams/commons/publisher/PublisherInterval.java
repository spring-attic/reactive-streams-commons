package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 */
public final class PublisherInterval extends Px<Long> {

    final BiFunction<Runnable, Long, ? extends Runnable> asyncExecutor;
    
    final LongSupplier now;
    
    final long initialDelay;
    
    final long period;
    
    final TimeUnit unit;

    public PublisherInterval(
            long initialDelay, 
            long period, 
            TimeUnit unit, 
            ScheduledExecutorService executor) {
        this(initialDelay, period, unit, Objects.requireNonNull(executor, "executor"), 1);
    }

    PublisherInterval(
            long initialDelay, 
            long period, 
            TimeUnit unit, 
            ScheduledExecutorService executor, int dummy) {
        this(initialDelay, period, unit, (r, d) -> {
            if (r != null) {
                Future<?> f = executor.schedule(r, d, unit);
                return () -> f.cancel(true);
            }
            return null;
        }, () -> unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    }

    public PublisherInterval(
            long initialDelay, 
            long period, 
            TimeUnit unit, 
            BiFunction<Runnable, Long, ? extends Runnable> asyncExecutor,
            LongSupplier now) {
        if (period < 0L) {
            throw new IllegalArgumentException("period >= 0 required but it was " + period);
        }
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = Objects.requireNonNull(unit, "unit");
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor, "asyncExecutor");
        this.now = Objects.requireNonNull(now, "now");
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        PublisherIntervalRunnable r = new PublisherIntervalRunnable(s, now.getAsLong() + initialDelay, period, asyncExecutor, now);
        
        s.onSubscribe(r);
        
        Runnable c = asyncExecutor.apply(r, initialDelay);
        
        r.setCancel(0, c);
    }
    
    static final class PublisherIntervalRunnable implements Runnable, Subscription {
        final Subscriber<? super Long> s;
        
        final BiFunction<Runnable, Long, ? extends Runnable> asyncExecutor;
        
        final LongSupplier now;
        
        volatile long requested;
        static final AtomicLongFieldUpdater<PublisherIntervalRunnable> REQUESTED = 
                AtomicLongFieldUpdater.newUpdater(PublisherIntervalRunnable.class, "requested");
        
        final long startTime;
        
        final long period;
        
        long count;

        volatile IndexedRunnable cancel;
        static final AtomicReferenceFieldUpdater<PublisherIntervalRunnable, IndexedRunnable> CANCEL =
                AtomicReferenceFieldUpdater.newUpdater(PublisherIntervalRunnable.class, IndexedRunnable.class, "cancel");
        
        static final IndexedRunnable CANCELLED = new IndexedRunnable(Long.MAX_VALUE, () -> { });

        public PublisherIntervalRunnable(Subscriber<? super Long> s, 
                long startTime, long period,
                BiFunction<Runnable, Long, ? extends Runnable> asyncExecutor,
                LongSupplier now) {
            this.s = s;
            this.startTime = startTime;
            this.period = period;
            this.now = now;
            this.asyncExecutor = asyncExecutor;
        }
        
        @Override
        public void run() {
            long c = count;
            long r = requested;
            if (r != 0L) {
                s.onNext(c);
                if (r != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }
            } else {
                s.onError(new IllegalStateException("Could not emit " + c + " due to lack of requests"));
                return;
            }
            
            long t = now.getAsLong();
            long next = startTime + (c + 1) * period;
            
            long delta = Math.max(next - t, 0);
            
            count = c + 1;
            
            setCancel(c + 1, asyncExecutor.apply(this, delta));
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
            }
        }
        
        @Override
        public void cancel() {
            IndexedRunnable a = cancel;
            if (a != CANCELLED) {
                a = CANCEL.getAndSet(this, CANCELLED);
                if (a != null && a != CANCELLED) {
                    a.run.run();
                }
            }
            asyncExecutor.apply(null, null);
        }
        
        public void setCancel(long index, Runnable r) {
            for (;;) {
                IndexedRunnable a = cancel;
                if (a != null && a.index > index) {
                    return;
                }
                IndexedRunnable b = new IndexedRunnable(index, r);
                if (CANCEL.compareAndSet(this, a, b)) {
                    return;
                }
            }
        }
    }
    
    static final class IndexedRunnable {
        
        final long index;
        
        final Runnable run;

        public IndexedRunnable(long index, Runnable run) {
            this.index = index;
            this.run = run;
        }
    }
}
