package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
public final class PublisherSubscribeOn<T> extends PublisherSource<T, T> {

    final Callable<Function<Runnable, Runnable>> schedulerFactory;
    
    public PublisherSubscribeOn(
            Publisher<? extends T> source, 
            Callable<Function<Runnable, Runnable>> schedulerFactory) {
        super(source);
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
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
        
        if (source instanceof Supplier) {
            
            @SuppressWarnings("unchecked")
            Supplier<T> supplier = (Supplier<T>) source;
            
            T v = supplier.get();
            
            if (v == null) {
                ScheduledEmptySubscriptionEager parent = new ScheduledEmptySubscriptionEager(s, scheduler);
                s.onSubscribe(parent);
                scheduler.apply(parent);
            } else {
                s.onSubscribe(new ScheduledSubscriptionEagerCancel<>(s, v, scheduler));
            }
            return;
        }
        
        PublisherSubscribeOnClassic<T> parent = new PublisherSubscribeOnClassic<>(s, scheduler);
        s.onSubscribe(parent);
        
        scheduler.apply(new SourceSubscribeTask<>(parent, source));
    }
    
    static final class PublisherSubscribeOnClassic<T>
    extends DeferredSubscription implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final Function<Runnable, Runnable> scheduler;

        public PublisherSubscribeOnClassic(Subscriber<? super T> actual, Function<Runnable, Runnable> scheduler) {
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
            scheduler.apply(null);
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            scheduler.apply(null);
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                scheduler.apply(() -> requestInner(n));
            }
        }
        
        @Override
        public void cancel() {
            super.cancel();
            scheduler.apply(null);
        }
        
        void requestInner(long n) {
            super.request(n);
        }
    }
    
    static final class ScheduledSubscriptionEagerCancel<T> implements Subscription, Runnable {

        final Subscriber<? super T> actual;
        
        final T value;
        
        final Function<Runnable, Runnable> scheduler;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ScheduledSubscriptionEagerCancel> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ScheduledSubscriptionEagerCancel.class, "once");

        volatile Runnable future;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ScheduledSubscriptionEagerCancel, Runnable> FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(ScheduledSubscriptionEagerCancel.class, Runnable.class, "future");

        public ScheduledSubscriptionEagerCancel(Subscriber<? super T> actual, T value, Function<Runnable, Runnable> scheduler) {
            this.actual = actual;
            this.value = value;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (ONCE.compareAndSet(this, 0, 1)) {
                    Runnable f = scheduler.apply(this);
                    if (!FUTURE.compareAndSet(this, null, f)) {
                        f.run();
                    }
                }
            }
        }
        
        @Override
        public void cancel() {
            ONCE.lazySet(this, 1);
            scheduler.apply(null);
        }
        
        @Override
        public void run() {
            actual.onNext(value);
            scheduler.apply(null);
            actual.onComplete();
        }
    }

    static final class ScheduledEmptySubscriptionEager implements Subscription, Runnable {
        final Subscriber<?> actual;

        final Function<Runnable, Runnable> scheduler;

        public ScheduledEmptySubscriptionEager(Subscriber<?> actual, Function<Runnable, Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            SubscriptionHelper.validate(n);
        }
        
        @Override
        public void cancel() {
            scheduler.apply(null);
        }
        
        @Override
        public void run() {
            scheduler.apply(null);
            actual.onComplete();
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

    static final class SourceSubscribeTaskScheduled<T> implements Runnable {

        final Subscriber<? super T> actual;
        
        final Publisher<? extends T> source;
        
        final Function<Runnable, Runnable> scheduler;

        public SourceSubscribeTaskScheduled(Subscriber<? super T> s, Publisher<? extends T> source, Function<Runnable, Runnable> scheduler) {
            this.actual = s;
            this.source = source;
            this.scheduler = scheduler;
        }

        @Override
        public void run() {
            source.subscribe(actual);
            scheduler.apply(null);
        }
    }
}
