package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
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

    final Callable<? extends Consumer<Runnable>> schedulerFactory;
    
    public PublisherSubscribeOn(
            Publisher<? extends T> source, 
            Callable<? extends Consumer<Runnable>> schedulerFactory) {
        super(source);
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
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
        
        if (source instanceof Supplier) {
            
            @SuppressWarnings("unchecked")
            Supplier<T> supplier = (Supplier<T>) source;
            
            T v = supplier.get();
            
            if (v == null) {
                ScheduledEmptySubscriptionEager parent = new ScheduledEmptySubscriptionEager(s, scheduler);
                s.onSubscribe(parent);
                scheduler.accept(parent);
            } else {
                s.onSubscribe(new ScheduledSubscriptionEagerCancel<>(s, v, scheduler));
            }
            return;
        }
        
        PublisherSubscribeOnClassic<T> parent = new PublisherSubscribeOnClassic<>(s, scheduler);
        s.onSubscribe(parent);
        
        scheduler.accept(new SourceSubscribeTask<>(parent, source));
    }
    
    static final class PublisherSubscribeOnClassic<T>
    extends DeferredSubscription implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final Consumer<Runnable> scheduler;

        public PublisherSubscribeOnClassic(Subscriber<? super T> actual, Consumer<Runnable> scheduler) {
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
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                scheduler.accept(new RequestTask(n, this));
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            scheduler.accept(null);
        }
        
        void requestInner(long n) {
            super.request(n);
        }

        static final class RequestTask implements Runnable {

            final long n;
            final PublisherSubscribeOnClassic parent;

            public RequestTask(long n, PublisherSubscribeOnClassic parent) {
                this.n = n;
                this.parent = parent;
            }

            @Override
            public void run() {
                parent.requestInner(n);
            }
        }
    }
    
    static final class ScheduledSubscriptionEagerCancel<T> implements Subscription, Runnable {

        final Subscriber<? super T> actual;
        
        final T value;
        
        final Consumer<Runnable> scheduler;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ScheduledSubscriptionEagerCancel> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ScheduledSubscriptionEagerCancel.class, "once");

        public ScheduledSubscriptionEagerCancel(Subscriber<? super T> actual, T value, Consumer<Runnable> scheduler) {
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
            ONCE.lazySet(this, 1);
            scheduler.accept(null);
        }
        
        @Override
        public void run() {
            actual.onNext(value);
            scheduler.accept(null);
            actual.onComplete();
        }
    }

    static final class ScheduledEmptySubscriptionEager implements Subscription, Runnable {
        final Subscriber<?> actual;

        final Consumer<Runnable> scheduler;

        public ScheduledEmptySubscriptionEager(Subscriber<?> actual, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            SubscriptionHelper.validate(n);
        }
        
        @Override
        public void cancel() {
            scheduler.accept(null);
        }
        
        @Override
        public void run() {
            scheduler.accept(null);
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

}
