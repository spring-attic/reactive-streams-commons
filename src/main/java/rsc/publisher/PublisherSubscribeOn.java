package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import rsc.documentation.*;
import rsc.flow.*;
import rsc.scheduler.Scheduler;
import rsc.scheduler.Scheduler.Worker;
import rsc.subscriber.DeferredSubscription;

import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 */
@FusionSupport(input = { FusionMode.SCALAR }, output = { FusionMode.ASYNC })
public final class PublisherSubscribeOn<T> extends PublisherSource<T, T> implements Loopback, Fuseable {

    final Scheduler scheduler;
    
    public PublisherSubscribeOn(
            Publisher<? extends T> source, 
            Scheduler scheduler) {
        super(source);
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (source instanceof Fuseable.ScalarCallable) {
            PublisherSubscribeOnValue.singleScheduleOn(source, s, scheduler);
            return;
        }
        
        Worker worker;
        
        try {
            worker = scheduler.createWorker();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            SubscriptionHelper.error(s, e);
            return;
        }
        
        if (worker == null) {
            SubscriptionHelper.error(s, new NullPointerException("The scheduler returned a null Function"));
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
            extends DeferredSubscription
            implements Subscriber<T>, Producer, Loopback, QueueSubscription<T> {
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
        
        @Override
        public int requestFusion(int requestedMode) {
            return Fuseable.NONE;
        }
        
        @Override
        public T poll() {
            return null;
        }
        
        @Override
        public boolean isEmpty() {
            return false;
        }
        
        @Override
        public int size() {
            return 0;
        }
        
        @Override
        public void clear() {
            
        }
    }

    static final class PublisherSubscribeOnPipeline<T>
            extends DeferredSubscription implements Subscriber<T>, Producer, Loopback, Runnable, QueueSubscription<T> {
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
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
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
        
        @Override
        public int requestFusion(int requestedMode) {
            return Fuseable.NONE;
        }
        
        @Override
        public T poll() {
            return null;
        }
        
        @Override
        public void clear() {
            
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
        
        @Override
        public int size() {
            return 0;
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
