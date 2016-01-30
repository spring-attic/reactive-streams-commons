package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.publisher.PublisherSubscribeOn.PublisherSubscribeOnClassic;
import reactivestreams.commons.publisher.PublisherSubscribeOn.ScheduledEmptySubscriptionEager;
import reactivestreams.commons.publisher.PublisherSubscribeOn.ScheduledSubscriptionEagerCancel;
import reactivestreams.commons.publisher.PublisherSubscribeOn.SourceSubscribeTask;
import reactivestreams.commons.publisher.PublisherSubscribeOn.SourceSubscribeTaskScheduled;
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
public final class PublisherSubscribeOnOther<T> extends PublisherSource<T, T> {

    static final Runnable CANCELLED = new Runnable() {
        @Override
        public void run() {

        }
    };

    final Callable<? extends Consumer<Runnable>> schedulerFactory;
    
    final boolean eagerCancel;
    
    final boolean requestOn;
    
    public PublisherSubscribeOnOther(
            Publisher<? extends T> source, 
            Callable<? extends Consumer<Runnable>> schedulerFactory,
            boolean eagerCancel, 
            boolean requestOn) {
        super(source);
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
        this.eagerCancel = eagerCancel;
        this.requestOn = requestOn;
    }

    static <T> boolean trySupplierScheduleOn(
            Publisher<? extends T> p, 
            Subscriber<? super T> s, 
            Consumer<Runnable> scheduler,
            boolean eagerCancel) {
        if (p instanceof Supplier) {
            
            @SuppressWarnings("unchecked")
            Supplier<T> supplier = (Supplier<T>) p;
            
            T v = supplier.get();
            
            supplierScheduleOnSubscribe(v, s, scheduler, eagerCancel);
            
            return true;
        }
        return false;
    }
    
    static <T> void supplierScheduleOnSubscribe(T v, final Subscriber<? super T> s, Consumer<Runnable>
            scheduler, boolean eagerCancel) {
        if (v == null) {
            if (eagerCancel) {
                ScheduledEmptySubscriptionEager parent = new ScheduledEmptySubscriptionEager(s, scheduler);
                s.onSubscribe(parent);
                scheduler.accept(parent);
            } else {
                scheduler.accept(new Runnable() {
                    @Override
                    public void run() {
                        EmptySubscription.complete(s);
                    }
                });
            }
        } else {
            if (eagerCancel) {
                s.onSubscribe(new ScheduledSubscriptionEagerCancel<>(s, v, scheduler));
            } else {
                s.onSubscribe(new ScheduledSubscriptionNonEagerCancel<>(s, v, scheduler));
            }
        }
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Consumer<Runnable> scheduler;
        
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

        if (trySupplierScheduleOn(source, s, scheduler, eagerCancel)) {
            return;
        }
        if (eagerCancel) {
            if (requestOn) {
                PublisherSubscribeOnClassic<T> parent = new PublisherSubscribeOnClassic<>(s, scheduler);
                s.onSubscribe(parent);
                
                scheduler.accept(new SourceSubscribeTask<>(parent, source));
            } else {
                PublisherSubscribeOnEagerDirect<T> parent = new PublisherSubscribeOnEagerDirect<>(s, scheduler);
                s.onSubscribe(parent);
                
                scheduler.accept(new SourceSubscribeTask<>(parent, source));
            }
        } else {
            if (requestOn) {
                scheduler.accept(new SourceSubscribeTask<>(new PublisherSubscribeOnNonEager<>(s, scheduler), source));
            } else {
                scheduler.accept(new SourceSubscribeTaskScheduled<>(s, source, scheduler));
            }
        }
    }
    
    static final class PublisherSubscribeOnNonEager<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;

        final Consumer<Runnable> scheduler;
        
        Subscription s;
        
        public PublisherSubscribeOnNonEager(Subscriber<? super T> actual,
                Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                
                this.s = s;
                
                actual.onSubscribe(this);
            }
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            scheduler.accept(null);
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            scheduler.accept(null);
            actual.onComplete();
        }
        
        @Override
        public void request(final long n) {
            scheduler.accept(new RequestTask(s, n));
        }
        
        @Override
        public void cancel() {
            s.cancel();
            scheduler.accept(null);
        }

        static final class RequestTask implements Runnable {

            final long n;
            final Subscription s;

            RequestTask(Subscription s, long n) {
                this.n = n;
                this.s = s;
            }

            @Override
            public void run() {
                s.request(n);
            }
        }
    }
    
    static final class PublisherSubscribeOnEagerDirect<T> 
    extends DeferredSubscription
    implements Subscriber<T> {
        final Subscriber<? super T> actual;

        final Consumer<Runnable> scheduler;

        public PublisherSubscribeOnEagerDirect(Subscriber<? super T> actual, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
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
            scheduler.accept(null);
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            scheduler.accept(null);
            actual.onComplete();
        }
        
        @Override
        public void cancel() {
            super.cancel();
            scheduler.accept(null);
        }
    }
    
    static final class ScheduledSubscriptionNonEagerCancel<T> implements Subscription, Runnable {

        final Subscriber<? super T> actual;
        
        final T value;
        
        final Consumer<Runnable> scheduler;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ScheduledSubscriptionNonEagerCancel> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ScheduledSubscriptionNonEagerCancel.class, "once");

        public ScheduledSubscriptionNonEagerCancel(Subscriber<? super T> actual, T value, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.value = value;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (ONCE.compareAndSet(this, 0, 1)) {
                    scheduler.accept(this);
                }
            }
        }
        
        @Override
        public void cancel() {
            once = 1;
            scheduler.accept(null);
        }
        
        @Override
        public void run() {
            actual.onNext(value);
            scheduler.accept(null);
            actual.onComplete();
        }
    }
}
