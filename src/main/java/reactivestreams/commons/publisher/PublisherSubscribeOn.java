package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.flow.Loopback;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.scheduler.Scheduler.Worker;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.DeferredSubscription;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 */
public final class PublisherSubscribeOn<T> extends PublisherSource<T, T> implements Loopback {

    final Scheduler scheduler;
    
    public PublisherSubscribeOn(
            Publisher<? extends T> source, 
            Scheduler scheduler) {
        super(source);
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public static <T> void scalarScheduleOn(Publisher<? extends T> source, Subscriber<? super T> s, Scheduler scheduler) {
        @SuppressWarnings("unchecked")
        Supplier<T> supplier = (Supplier<T>) source;
        
        T v = supplier.get();
        
        if (v == null) {
            ScheduledEmpty parent = new ScheduledEmpty(s);
            s.onSubscribe(parent);
            Runnable f = scheduler.schedule(parent);
            parent.setFuture(f);
        } else {
            s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
        }
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (source instanceof Supplier) {
            scalarScheduleOn(source, s, scheduler);
            return;
        }
        
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
        
        PublisherSubscribeOnClassic<T> parent = new PublisherSubscribeOnClassic<>(s, worker);
        s.onSubscribe(parent);
        
        worker.schedule(new SourceSubscribeTask<>(parent, source));
    }

    @Override
    public Object connectedInput() {
        return scheduler;
    }

    @Override
    public Object connectedOutput() {
        return null;
    }

    static final class PublisherSubscribeOnClassic<T>
            extends DeferredSubscription implements Subscriber<T>, Producer, Loopback {
        final Subscriber<? super T> actual;

        final Worker worker;

        public PublisherSubscribeOnClassic(Subscriber<? super T> actual, Worker worker) {
            this.actual = actual;
            this.worker = worker;
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            try {
                actual.onError(t);
            } finally {
                worker.shutdown();
            }
        }

        @Override
        public void onComplete() {
            try {
                actual.onComplete();
            } finally {
                worker.shutdown();
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                worker.schedule(new RequestTask(n, this));
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            worker.shutdown();
        }

        void requestInner(long n) {
            super.request(n);
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedOutput() {
            return worker;
        }

        @Override
        public Object connectedInput() {
            return null;
        }
    }

    static final class PublisherSubscribeOnPipeline<T>
            extends DeferredSubscription implements Subscriber<T>, Producer, Loopback, Runnable {
        final Subscriber<? super T> actual;

        final Worker worker;

        volatile long requested;

        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherSubscribeOnPipeline> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(PublisherSubscribeOnPipeline.class, "requested");

        volatile int wip;

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSubscribeOnPipeline> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSubscribeOnPipeline.class, "wip");

        public PublisherSubscribeOnPipeline(Subscriber<? super T> actual, Worker worker) {
            this.actual = actual;
            this.worker = worker;
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            try {
                actual.onError(t);
            } finally {
                worker.shutdown();
            }
        }

        @Override
        public void onComplete() {
            try {
                actual.onComplete();
            } finally {
                worker.shutdown();
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
                if(WIP.getAndIncrement(this) == 0){
                    worker.schedule(this);
                }
            }
        }

        @Override
        public void run() {
            long r;
            int missed = 1;
            for(;;){
                r = REQUESTED.getAndSet(this, 0L);

                if(r != 0L) {
                    super.request(r);
                }

                if(r == Long.MAX_VALUE){
                    return;
                }

                missed = WIP.addAndGet(this, -missed);
                if(missed == 0){
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            worker.shutdown();
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedOutput() {
            return worker;
        }

        @Override
        public Object connectedInput() {
            return null;
        }
    }

    static final class RequestTask implements Runnable {

        final long n;
        final PublisherSubscribeOnClassic<?> parent;

        public RequestTask(long n, PublisherSubscribeOnClassic<?> parent) {
            this.n = n;
            this.parent = parent;
        }

        @Override
        public void run() {
            parent.requestInner(n);
        }
    }
    
    static final class ScheduledScalar<T>
            implements Subscription, Runnable, Producer, Loopback {

        final Subscriber<? super T> actual;
        
        final T value;
        
        final Scheduler scheduler;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ScheduledScalar> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ScheduledScalar.class, "once");
        
        volatile Runnable future;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ScheduledScalar, Runnable> FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(ScheduledScalar.class, Runnable.class, "future");
        
        static final Runnable CANCELLED = () -> { };

        static final Runnable FINISHED = () -> { };

        public ScheduledScalar(Subscriber<? super T> actual, T value, Scheduler scheduler) {
            this.actual = actual;
            this.value = value;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (ONCE.compareAndSet(this, 0, 1)) {
                    Runnable f = scheduler.schedule(this);
                    if (!FUTURE.compareAndSet(this, null, f)) {
                        if (future != FINISHED && future != CANCELLED) {
                            f.run();
                        }
                    }
                }
            }
        }
        
        @Override
        public void cancel() {
            ONCE.lazySet(this, 1);
            Runnable f = future;
            if (f != CANCELLED && future != FINISHED) {
                f = FUTURE.getAndSet(this, CANCELLED);
                if (f != null && f != CANCELLED && f != FINISHED) {
                    f.run();
                }
            }
        }
        
        @Override
        public void run() {
            try {
                actual.onNext(value);
                actual.onComplete();
            } finally {
                FUTURE.lazySet(this, FINISHED);
            }
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedInput() {
            return scheduler;
        }

        @Override
        public Object connectedOutput() {
            return value;
        }
    }

    static final class ScheduledEmpty implements Subscription, Runnable, Producer, Loopback {
        final Subscriber<?> actual;

        volatile Runnable future;
        static final AtomicReferenceFieldUpdater<ScheduledEmpty, Runnable> FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(ScheduledEmpty.class, Runnable.class, "future");
        
        static final Runnable CANCELLED = () -> { };
        
        static final Runnable FINISHED = () -> { };

        public ScheduledEmpty(Subscriber<?> actual) {
            this.actual = actual;
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.validate(n);
        }

        @Override
        public void cancel() {
            Runnable f = future;
            if (f != CANCELLED && f != FINISHED) {
                f = FUTURE.getAndSet(this, CANCELLED);
                if (f != null && f != CANCELLED && f != FINISHED) {
                    f.run();
                }
            }
        }

        @Override
        public void run() {
            try {
                actual.onComplete();
            } finally {
                FUTURE.lazySet(this, FINISHED);
            }
        }

        void setFuture(Runnable f) {
            if (!FUTURE.compareAndSet(this, null, f)) {
                Runnable a = future;
                if (a != FINISHED && a != CANCELLED) {
                    f.run();
                }
            }
        }
        
        @Override
        public Object connectedInput() {
            return null; // FIXME value?
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }

    static final class SourceSubscribeTask<T> implements Runnable {

        final Subscriber<? super T> actual;
        
        final Publisher<? extends T> source;

        public SourceSubscribeTask(Subscriber<? super T> s, Publisher<? extends T> source) {
            this.actual = s;
            this.source = source;
        }

        @Override
        public void run() {
            source.subscribe(actual);
        }
    }

}
