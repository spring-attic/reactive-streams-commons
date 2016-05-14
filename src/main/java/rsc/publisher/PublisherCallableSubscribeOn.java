package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;

import org.reactivestreams.Subscriber;

import rsc.documentation.*;
import rsc.flow.*;
import rsc.scheduler.Scheduler;
import rsc.util.*;

/**
 * Executes a Callable and emits its value on the given Scheduler.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE }, output = { FusionMode.ASYNC, FusionMode.BOUNDARY })
public final class PublisherCallableSubscribeOn<T> extends Px<T> implements Fuseable {

    final Callable<? extends T> callable;
    
    final Scheduler scheduler;
    
    public PublisherCallableSubscribeOn(Callable<? extends T> callable,
            Scheduler scheduler) {
        this.callable = Objects.requireNonNull(callable, "callable");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        subscribe(callable, s, scheduler);
    }
    
    public static <T> void subscribe(Callable<T> callable, Subscriber<? super T> s, Scheduler scheduler) {
        CallableSubscribeOnSubscription<T> parent = new CallableSubscribeOnSubscription<>(s, callable, scheduler);
        s.onSubscribe(parent);
        
        Cancellation f = scheduler.schedule(parent);
        
        parent.setMainFuture(f);
    }
    
    static final class CallableSubscribeOnSubscription<T> implements QueueSubscription<T>, Runnable {
        final Subscriber<? super T> actual;
        
        final Callable<? extends T> callable;
        
        final Scheduler scheduler;

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<CallableSubscribeOnSubscription> STATE =
                AtomicIntegerFieldUpdater.newUpdater(CallableSubscribeOnSubscription.class, "state");
        
        T value;
        static final int NO_REQUEST_NO_VALUE = 0;
        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        static final int CANCELLED = 4;
        
        int fusionState;
        
        static final int NOT_FUSED = 0;
        static final int NO_VALUE = 1;
        static final int HAS_VALUE = 2;
        static final int COMPLETE = 3;
        
        volatile Cancellation mainFuture;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Cancellation> MAIN_FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(CallableSubscribeOnSubscription.class, Cancellation.class, "mainFuture");

        volatile Cancellation requestFuture;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<CallableSubscribeOnSubscription, Cancellation> REQUEST_FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(CallableSubscribeOnSubscription.class, Cancellation.class, "requestFuture");

        static final Cancellation CANCEL = () -> { };
        
        public CallableSubscribeOnSubscription(Subscriber<? super T> actual, 
                Callable<? extends T> callable, Scheduler scheduler) {
            this.actual = actual;
            this.callable = callable;
            this.scheduler = scheduler;
        }
        
        @Override
        public void cancel() {
            state = CANCELLED;
            fusionState = COMPLETE;
            Cancellation a = mainFuture;
            if (a != CANCEL) {
                a = MAIN_FUTURE.getAndSet(this, CANCEL);
                if (a != null && a != CANCEL) {
                    a.dispose();
                }
            }
            a = requestFuture;
            if (a != CANCEL) {
                a = REQUEST_FUTURE.getAndSet(this, CANCEL);
                if (a != null && a != CANCEL) {
                    a.dispose();
                }
            }
        }
        
        @Override
        public void clear() {
            value = null;
            fusionState = COMPLETE;
        }
        
        @Override
        public boolean isEmpty() {
            return fusionState != HAS_VALUE;
        }
        
        @Override
        public T poll() {
            if (fusionState == HAS_VALUE) {
                fusionState = COMPLETE;
                return value;
            }
            return null;
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            if ((requestedMode & ASYNC) != 0 && (requestedMode & THREAD_BARRIER) == 0) {
                fusionState = NO_VALUE;
                return ASYNC;
            }
            return NONE;
        }
        
        @Override
        public int size() {
            return isEmpty() ? 0 : 1;
        }
        
        void setMainFuture(Cancellation c) {
            for (;;) {
                Cancellation a = mainFuture;
                if (a == CANCEL) {
                    c.dispose();
                    return;
                }
                if (MAIN_FUTURE.compareAndSet(this, a, c)) {
                    return;
                }
            }
        }

        void setRequestFuture(Cancellation c) {
            for (;;) {
                Cancellation a = requestFuture;
                if (a == CANCEL) {
                    c.dispose();
                    return;
                }
                if (REQUEST_FUTURE.compareAndSet(this, a, c)) {
                    return;
                }
            }
        }

        @Override
        public void run() {
            T v;
            
            try {
                v = callable.call();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                actual.onError(ex);
                return;
            }
            
            for (;;) {
                int s = state;
                if (s == CANCELLED || s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
                    return;
                }
                if (s == HAS_REQUEST_NO_VALUE) {
                    if (fusionState == NO_VALUE) {
                        this.value = v;
                        this.fusionState = HAS_VALUE;
                    }
                    actual.onNext(v);
                    if (state != CANCELLED) {
                        actual.onComplete();
                    }
                    return;
                }
                this.value = v;
                if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
                    return;
                }
            }
        }

        void emit(T v) {
            
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                for (;;) {
                    int s = state;
                    if (s == CANCELLED || s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (s == NO_REQUEST_HAS_VALUE) {
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                            Cancellation f = scheduler.schedule(this::emitValue);
                            setRequestFuture(f);
                        }
                        return;
                    }
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }
        
        void emitValue() {
            if (fusionState == NO_VALUE) {
                this.fusionState = HAS_VALUE;
            }
            actual.onNext(value);
            if (state != CANCELLED) {
                actual.onComplete();
            }
        }

    }
}
