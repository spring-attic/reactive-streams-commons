package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.*;

import org.reactivestreams.Subscriber;

import rsc.flow.*;
import rsc.subscriber.SignalEmitter;
import rsc.util.*;
import rsc.flow.Fuseable.*;

/**
 * Generate signals one-by-one via a function callback.
 * <p>
 * <p>
 * The {@code stateSupplier} may return {@code null} but your {@code stateConsumer} should be prepared to
 * handle it.
 *
 * @param <T> the value type emitted
 * @param <S> the custom state per subscriber
 */
@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE }, output = { FusionMode.SYNC, FusionMode.BOUNDARY })
public final class PublisherGenerate<T, S> 
extends Px<T> {

    final Callable<S> stateSupplier;

    final BiFunction<S, SignalEmitter<T>, S> generator;

    final Consumer<? super S> stateConsumer;

    public PublisherGenerate(BiFunction<S, SignalEmitter<T>, S> generator) {
        this(() -> null, generator, s -> {
        });
    }

    public PublisherGenerate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator) {
        this(stateSupplier, generator, s -> {
        });
    }

    public PublisherGenerate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator,
                             Consumer<? super S> stateConsumer) {
        this.stateSupplier = Objects.requireNonNull(stateSupplier, "stateSupplier");
        this.generator = Objects.requireNonNull(generator, "generator");
        this.stateConsumer = Objects.requireNonNull(stateConsumer, "stateConsumer");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        S state;

        try {
            state = stateSupplier.call();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        s.onSubscribe(new GenerateSubscription<>(s, state, generator, stateConsumer));
    }

    static final class GenerateSubscription<T, S>
      implements QueueSubscription<T>, SignalEmitter<T> {

        final Subscriber<? super T> actual;

        final BiFunction<S, SignalEmitter<T>, S> generator;

        final Consumer<? super S> stateConsumer;

        volatile boolean cancelled;

        S state;

        boolean terminate;

        boolean hasValue;
        
        boolean outputFused;
        
        T generatedValue;
        
        Throwable generatedError;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<GenerateSubscription> REQUESTED = 
            AtomicLongFieldUpdater.newUpdater(GenerateSubscription.class, "requested");

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        public GenerateSubscription(Subscriber<? super T> actual, S state,
                                             BiFunction<S, SignalEmitter<T>, S> generator, Consumer<? super
          S> stateConsumer) {
            this.actual = actual;
            this.state = state;
            this.generator = generator;
            this.stateConsumer = stateConsumer;
        }

        @Override
        public Emission emit(T t) {
            if (terminate) {
                return Emission.CANCELLED;
            }
            if (hasValue) {
                fail(new IllegalStateException("More than one call to onNext"));
                return Emission.FAILED;
            }
            if (t == null) {
                fail(new NullPointerException("The generator produced a null value"));
                return Emission.FAILED;
            }
            hasValue = true;
            if (outputFused) {
                generatedValue = t;
            } else {
                actual.onNext(t);
            }
            return Emission.OK;
        }

        @Override
        public void fail(Throwable e) {
            if (terminate) {
                return;
            }
            terminate = true;
            if (outputFused) {
                generatedError = e;
            } else {
                actual.onError(e);
            }
        }

        @Override
        public void complete() {
            if (terminate) {
                return;
            }
            terminate = true;
            if (!outputFused) {
                actual.onComplete();
            }
        }

        @Override
        public void stop() {
            if (terminate) {
                return;
            }
            terminate = true;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.getAndAddCap(REQUESTED, this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }

        void fastPath() {
            S s = state;

            final BiFunction<S, SignalEmitter<T>, S> g = generator;

            for (; ; ) {

                if (cancelled) {
                    cleanup(s);
                    return;
                }

                try {
                    s = g.apply(s, this);
                } catch (Throwable e) {
                    cleanup(s);

                    actual.onError(e);
                    return;
                }
                if (terminate || cancelled) {
                    cleanup(s);
                    return;
                }
                if (!hasValue) {
                    cleanup(s);

                    actual.onError(new IllegalStateException("The generator didn't call any of the " +
                      "PublisherGenerateOutput method"));
                    return;
                }

                hasValue = false;
            }
        }

        void slowPath(long n) {
            S s = state;

            long e = 0L;

            final BiFunction<S, SignalEmitter<T>, S> g = generator;

            for (; ; ) {
                while (e != n) {

                    if (cancelled) {
                        cleanup(s);
                        return;
                    }

                    try {
                        s = g.apply(s, this);
                    } catch (Throwable ex) {
                        cleanup(s);

                        actual.onError(ex);
                        return;
                    }
                    if (terminate || cancelled) {
                        cleanup(s);
                        return;
                    }
                    if (!hasValue) {
                        cleanup(s);

                        actual.onError(new IllegalStateException("The generator didn't call any of the " +
                          "PublisherGenerateOutput method"));
                        return;
                    }

                    e++;
                    hasValue = false;
                }

                n = requested;

                if (n == e) {
                    state = s;
                    n = REQUESTED.addAndGet(this, -e);
                    if (n == 0L) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;

                if (REQUESTED.getAndIncrement(this) == 0) {
                    cleanup(state);
                }
            }
        }

        void cleanup(S s) {
            try {
                state = null;

                stateConsumer.accept(s);
            } catch (Throwable e) {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            if ((requestedMode & Fuseable.SYNC) != 0 && (requestedMode & Fuseable.THREAD_BARRIER) == 0) {
                outputFused = true;
                return Fuseable.SYNC;
            }
            return Fuseable.NONE;
        }
        
        @Override
        public T poll() {
            S s = state;

            if (terminate) {
                cleanup(s);
                
                Throwable e = generatedError;
                if (e != null) {
                    
                    generatedError = null;
                    ExceptionHelper.bubble(e);
                }
                
                return null;
            }

            
            try {
                s = generator.apply(s, this);
            } catch (final Throwable ex) {
                cleanup(s);
                throw ex;
            }
            
            if (!hasValue) {
                cleanup(s);
                
                if (!terminate) {
                    throw new IllegalStateException("The generator didn't call any of the " +
                        "PublisherGenerateOutput method");
                }
                
                Throwable e = generatedError;
                if (e != null) {
                    
                    generatedError = null;
                    throw ExceptionHelper.bubble(e);
                }
                
                return null;
            }
            
            T v = generatedValue;
            generatedValue = null;
            hasValue = false;

            state = s;
            return v;
        }

        @Override
        public Throwable getError() {
            return generatedError;
        }

        @Override
        public boolean isEmpty() {
            return terminate;
        }
        
        @Override
        public int size() {
            return isEmpty() ? 0 : -1;
        }
        
        @Override
        public void clear() {
            generatedError = null;
            generatedValue = null;
        }
    }
}
