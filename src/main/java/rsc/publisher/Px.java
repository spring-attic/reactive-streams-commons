package rsc.publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.Cancellation;
import rsc.flow.Fuseable;
import rsc.parallel.ParallelPublisher;
import rsc.scheduler.ExecutorServiceScheduler;
import rsc.scheduler.Scheduler;
import rsc.scheduler.TimedScheduler;
import rsc.state.Introspectable;
import rsc.subscriber.BlockingFirstSubscriber;
import rsc.subscriber.BlockingLastSubscriber;
import rsc.subscriber.EmptyAsyncSubscriber;
import rsc.subscriber.LambdaSubscriber;
import rsc.subscriber.PeekLastSubscriber;
import rsc.subscriber.SignalEmitter;
import rsc.test.TestSubscriber;
import rsc.util.SpscArrayQueue;
import rsc.util.SpscLinkedArrayQueue;
import rsc.util.UnsignalledExceptions;

/**
 * Experimental base class with fluent API: (P)ublisher E(x)tensions.
 *
 * <p>
 * Remark: in Java 8, this could be an interface with default methods but some library
 * users need Java 7.
 * 
 * <p>
 * Use {@link #wrap(Publisher)} to wrap any Publisher. 
 * 
 * @param <T> the output value type
 */
public abstract class Px<T> implements Publisher<T>, Introspectable {

    static final int BUFFER_SIZE = 128;
    
    /**
     * If set to true, applying operators on this will inject an
     * intermediate PublisherOnAssembly operator that captures
     * the current stacktrace. The stacktrace is then
     * visible as string in debug time and gets appended to
     * all passing onError signal.
     */
    public static volatile boolean trackAssembly;
    
    
    /**
     * Wrap the source into a PublisherOnAssembly or PublisherCallableOnAssembly if 
     * {@code trackAssembly} is set to true.
     * @param <T> the value type
     * @param source the source to wrap
     * @return the potentially wrapped source
     */
    static <T> Px<T> onAssembly(Px<T> source) {
        if (trackAssembly) {
            if (source instanceof Callable) {
                return new PublisherCallableOnAssembly<>(source);
            }
            return new PublisherOnAssembly<>(source);
        }
        return source;
    }
    
    /**
     * Wrap the source into a ConnectablePublisherOnAssembly if 
     * {@code trackAssembly} is set to true.
     * @param <T> the value type
     * @param source the source to wrap
     * @return the potentially wrapped source
     */
    static <T> ConnectablePublisher<T> onAssembly(ConnectablePublisher<T> source) {
        if (trackAssembly) {
            return new ConnectablePublisherOnAssembly<>(source);
        }
        return source;
    }
    
    static final Supplier<Queue<Object>> QUEUE_SUPPLIER = new Supplier<Queue<Object>>() {
        @Override
        public Queue<Object> get() {
            return new ConcurrentLinkedQueue<>();
        }
    };
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Supplier<Queue<T>> defaultQueueSupplier(final int capacity) {
        if (capacity == Integer.MAX_VALUE) {
            return (Supplier)QUEUE_SUPPLIER;
        }
        return new Supplier<Queue<T>>() {
            @Override
            public Queue<T> get() {
                return new SpscArrayQueue<>(capacity);
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Supplier<Queue<T>> defaultUnboundedQueueSupplier(final int capacity) {
        if (capacity == Integer.MAX_VALUE) {
            return (Supplier)QUEUE_SUPPLIER;
        }
        return new Supplier<Queue<T>>() {
            @Override
            public Queue<T> get() {
                return new SpscLinkedArrayQueue<>(capacity);
            }
        };
    }
    
    public static int bufferSize() {
        return BUFFER_SIZE;
    }

    public final <R> Px<R> map(Function<? super T, ? extends R> mapper) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherMapFuseable<>(this, mapper));
        }
        return onAssembly(new PublisherMap<>(this, mapper));
    }
    
    public final Px<T> filter(Predicate<? super T> predicate) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherFilterFuseable<>(this, predicate));
        }
        return onAssembly(new PublisherFilter<>(this, predicate));
    }
    
    public final Px<T> take(long n) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherTakeFuseable<>(this, n));
        }
        return onAssembly(new PublisherTake<>(this, n));
    }
    
    public final Px<T> concatWith(Publisher<? extends T> other) {
        return onAssembly(new PublisherConcatArray<>(false, this, other));
    }
    
    public final Px<T> ambWith(Publisher<? extends T> other) {
        return onAssembly(new PublisherAmb<>(this, other));
    }
    
    public final <U, R> Px<R> withLatestFrom(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return onAssembly(new PublisherWithLatestFrom<>(this, other, combiner));
    }
    
    public final <R> Px<R> switchMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return onAssembly(new PublisherSwitchMap<>(this, mapper, defaultQueueSupplier(Integer.MAX_VALUE), BUFFER_SIZE));
    }
    
    public final Px<T> retryWhen(Function<? super Px<Throwable>, ? extends Publisher<? extends Object>> whenFunction) {
        return onAssembly(new PublisherRetryWhen<>(this, whenFunction));
    }

    public final Px<T> repeatWhen(Function<? super Px<Long>, ? extends Publisher<? extends
            Object>> whenFunction) {
        return onAssembly(new PublisherRepeatWhen<>(this, whenFunction));
    }

    public final <U> Px<List<T>> buffer(Publisher<U> other) {
        return buffer(other, () -> new ArrayList<>());
    }
    
    public final <U, C extends Collection<? super T>> Px<C> buffer(Publisher<U> other, Supplier<C> bufferSupplier) {
        return onAssembly(new PublisherBufferBoundary<>(this, other, bufferSupplier));
    }

    public final <U, V> Px<List<T>> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return buffer(start, end, () -> new ArrayList<>());
    }

    public final <U, V, C extends Collection<? super T>> Px<C> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end, 
                    Supplier<C> bufferSupplier) {
        return onAssembly(new PublisherBufferStartEnd<>(this, start, end, bufferSupplier, defaultQueueSupplier(Integer.MAX_VALUE)));
    }

    public final Px<Px<T>> window(int size) {
        return onAssembly(new PublisherWindow<>(this, size, defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    public final Px<Px<T>> window(int size, int skip) {
        return onAssembly(new PublisherWindow<>(this, size, skip, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    public final <U> Px<Px<T>> window(Publisher<U> other) {
        return onAssembly(new PublisherWindowBoundary<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    public final <U> Px<Px<T>> window(Publisher<U> other, int maxSize) {
        return onAssembly(new PublisherWindowBoundaryAndSize<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize));
    }

    public final <U> Px<Px<T>> window(Publisher<U> other, int maxSize, boolean allowEmptyWindows) {
        if (!allowEmptyWindows) {
            return onAssembly(new PublisherWindowBoundaryAndSizeNonEmpty<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize));
        }
        return onAssembly(new PublisherWindowBoundaryAndSize<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize));
    }

    public final Px<T> accumulate(BiFunction<T, ? super T, T> accumulator) {
        return onAssembly(new PublisherAccumulate<>(this, accumulator));
    }
    
    public final Px<Boolean> all(Predicate<? super T> predicate) {
        return onAssembly(new PublisherAll<>(this, predicate));
    }
    
    public final Px<Boolean> any(Predicate<? super T> predicate) {
        return onAssembly(new PublisherAny<>(this, predicate));
    }
    
    public final Px<Boolean> exists(T value) {
        return any(v -> Objects.equals(v, value));
    }
    
    public final Px<List<T>> buffer(int count) {
        return onAssembly(new PublisherBuffer<>(this, count, () -> new ArrayList<>()));
    }
    
    public final Px<List<T>> buffer(int count, int skip) {
        return onAssembly(new PublisherBuffer<>(this, count, skip, () -> new ArrayList<>()));
    }
    
    public final <C extends Collection<? super T>> Px<C> buffer(int count, int skip, Supplier<C> bufferSupplier) {
        return onAssembly(new PublisherBuffer<>(this, count, skip, bufferSupplier));
    }

    public final <R> Px<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> collector) {
        return onAssembly(new PublisherCollect<>(this, supplier, collector));
    }
    
    public final Px<List<T>> toList() {
        return collect(() -> new ArrayList<>(), (a, b) -> a.add(b));
    }
    
    public final Px<Long> count() {
        return onAssembly(new PublisherCount<>(this));
    }
    
    public final Px<T> defaultIfEmpty(T value) {
        return onAssembly(new PublisherDefaultIfEmpty<>(this, value));
    }
    
    public final <U> Px<T> delaySubscription(Publisher<U> other) {
        return onAssembly(new PublisherDelaySubscription<>(this, other));
    }
    
    public final Px<T> distinct() {
        return distinct(v -> v);
    }
    
    public final <K> Px<T> distinct(Function<? super T, K> keyExtractor) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherDistinctFuseable<>(this, keyExtractor, () -> new HashSet<>()));
        }
        return onAssembly(new PublisherDistinct<>(this, keyExtractor, () -> new HashSet<>()));
    }
    
    public final Px<T> distinctUntilChanged() {
        return distinctUntilChanged(v -> v);
    }
    
    public final <K> Px<T> distinctUntilChanged(Function<? super T, K> keyExtractor) {
        return onAssembly(new PublisherDistinctUntilChanged<>(this, keyExtractor));
    }
    
    public final Px<T> onBackpressureDrop() {
        return onAssembly(new PublisherDrop<>(this));
    }
    
    public final Px<T> elementAt(long index) {
        return onAssembly(new PublisherElementAt<>(this, index));
    }
    
    public final Px<T> elementAt(long index, T defaultValue) {
        return onAssembly(new PublisherElementAt<>(this, index, () -> defaultValue));
    }
    
    public final Px<T> ignoreElements() {
        return onAssembly(new PublisherIgnoreElements<>(this));
    }
    
    public final Px<T> onBackpressureLatest() {
        return onAssembly(new PublisherLatest<>(this));
    }
    
    public final <R> Px<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> onLift) {
        return onAssembly(new PublisherLift<>(this, onLift));
    }
    
    public final Px<T> next() {
        return onAssembly(new PublisherNext<>(this));
    }

    public final Px<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, onSubscribe, null, null, null, null, null, null));
        }
        return onAssembly(new PublisherPeek<>(this, onSubscribe, null, null, null, null, null,
                null));
    }
    
    public final Px<T> doOnNext(Consumer<? super T> onNext) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, onNext, null, null, null, null, null));
        }
        return onAssembly(new PublisherPeek<>(this, null, onNext, null, null, null, null, null));
    }

    public final Px<T> doOnError(Consumer<? super Throwable> onError) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, null, onError, null, null, null, null));
        }
        return onAssembly(new PublisherPeek<>(this, null, null, onError, null, null, null, null));
    }

    public final Px<T> doOnComplete(Runnable onComplete) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, null, null, onComplete, null, null, null));
        }
        return onAssembly(new PublisherPeek<>(this, null, null, null, onComplete, null, null, null));
    }
    
    public final Px<T> doAfterTerminate(Runnable onAfterTerminate) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, null, null, null, onAfterTerminate, null, null));
        }
        return onAssembly(new PublisherPeek<>(this, null, null, null, null, onAfterTerminate, null, null));
    }

    public final Px<T> doOnRequest(LongConsumer onRequest) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, null, null, null, null, onRequest, null));
        }
        return onAssembly(new PublisherPeek<>(this, null, null, null, null, null, onRequest, null));
    }
    
    public final Px<T> doOnCancel(Runnable onCancel) {
        if (this instanceof Fuseable) {
            return onAssembly(new PublisherPeekFuseable<>(this, null, null, null, null, null, null, onCancel));
        }
        return onAssembly(new PublisherPeek<>(this, null, null, null, null, null, null, onCancel));
    }

    public final <R> Px<R> reduce(Supplier<R> initialValue, BiFunction<R, ? super T, R> accumulator) {
        return onAssembly(new PublisherReduce<>(this, initialValue, accumulator));
    }
    
    public final Px<T> repeat() {
        return onAssembly(new PublisherRepeat<>(this));
    }

    public final Px<T> repeat(long times) {
        return onAssembly(new PublisherRepeat<>(this, times));
    }

    public final Px<T> repeat(BooleanSupplier predicate) {
        return onAssembly(new PublisherRepeatPredicate<>(this, predicate));
    }

    public final Px<T> retry() {
        return onAssembly(new PublisherRetry<>(this));
    }

    public final Px<T> retry(long times) {
        return onAssembly(new PublisherRetry<>(this, times));
    }

    public final Px<T> retry(Predicate<Throwable> predicate) {
        return onAssembly(new PublisherRetryPredicate<>(this, predicate));
    }
    
    public final Px<T> onErrorReturn(T value) {
        return onErrorResumeNext(new PublisherJust<>(value));
    }
    
    public final Px<T> onErrorResumeNext(Publisher<? extends T> next) {
        return onErrorResumeNext(e -> next);
    }
    
    public final Px<T> onErrorResumeNext(Function<Throwable, ? extends Publisher<? extends T>> nextFunction) {
        return onAssembly(new PublisherResume<>(this, nextFunction));
    }
    
    public final <U> Px<T> sample(Publisher<U> sampler) {
        return onAssembly(new PublisherSample<>(this, sampler));
    }
    
    public final <R> Px<R> scan(R initialValue, BiFunction<R, ? super T, R> accumulator) {
        return onAssembly(new PublisherScan<>(this, initialValue, accumulator));
    }
    
    public final Px<T> single() {
        return onAssembly(new PublisherSingle<>(this));
    }

    public final Px<T> single(T defaultValue) {
        return onAssembly(new PublisherSingle<>(this, () -> defaultValue));
    }

    public final Px<T> skip(long n) {
        return onAssembly(new PublisherSkip<>(this, n));
    }
    
    public final Px<T> skipLast(int n) {
        return onAssembly(new PublisherSkipLast<>(this, n));
    }
    
    public final Px<T> skipWhile(Predicate<? super T> predicate) {
        return onAssembly(new PublisherSkipWhile<>(this, predicate));
    }

    public final <U> Px<T> skipUntil(Publisher<U> other) {
        return onAssembly(new PublisherSkipUntil<>(this, other));
    }
    
    public final Px<T> switchIfEmpty(Publisher<? extends T> other) {
        return onAssembly(new PublisherSwitchIfEmpty<>(this, other));
    }
    
    public final <U, V> Px<Px<T>> window(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return onAssembly(new PublisherWindowStartEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    public final <U, V> Px<Px<T>> window2(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return onAssembly(new PublisherWindowBeginEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE));
    }

    public final Px<T> takeLast(int n) {
        if (n == 1) {
            return onAssembly(new PublisherTakeLastOne<>(this));
        }
        return onAssembly(new PublisherTakeLast<>(this, n));
    }
    
    public final <U> Px<T> takeUntil(Publisher<U> other) {
        return onAssembly(new PublisherTakeUntil<>(this, other));
    }
    
    public final Px<T> takeUntil(Predicate<? super T> predicate) {
        return onAssembly(new PublisherTakeUntilPredicate<>(this, predicate));
    }

    public final Px<T> takeWhile(Predicate<? super T> predicate) {
        return onAssembly(new PublisherTakeWhile<>(this, predicate));
    }
    
    public final <U, V> Px<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout) {
        return onAssembly(new PublisherTimeout<>(this, firstTimeout, itemTimeout));
    }

    public final <U, V> Px<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout, Publisher<? extends T> other) {
        return onAssembly(new PublisherTimeout<>(this, firstTimeout, itemTimeout, other));
    }

    public final <U, R> Px<R> zipWith(Iterable<U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        return onAssembly(new PublisherZipIterable<>(this, other, zipper));
    }
    
    public final <U> Px<T> throttleFirst(Function<? super T, ? extends Publisher<U>> throttler) {
        return onAssembly(new PublisherThrottleFirst<>(this, throttler));
    }
    
    public final <U> Px<T> throttleLast(Publisher<U> throttler) {
        return sample(throttler);
    }
    
    public final <U> Px<T> throttleTimeout(Function<? super T, ? extends Publisher<U>> throttler) {
        return onAssembly(new PublisherThrottleTimeout<>(this, throttler, defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }
    
    public final Iterable<T> toIterable() {
        return toIterable(BUFFER_SIZE);
    }

    public final Iterable<T> toIterable(long batchSize) {
        return new BlockingIterable<>(this, batchSize, defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }
    
    public final Stream<T> stream() {
        return stream(BUFFER_SIZE);
    }
    
    public final Stream<T> stream(long batchSize) {
        return new BlockingIterable<>(this, batchSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)).stream();
    }

    public final Stream<T> parallelStream() {
        return parallelStream(BUFFER_SIZE);
    }
    
    public final Stream<T> parallelStream(long batchSize) {
        return new BlockingIterable<>(this, batchSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)).parallelStream();
    }

    public final Future<T> toFuture() {
        return new BlockingFuture<>(this).future();
    }
    
    public final Future<T> toFuture(T defaultValue) {
        return new BlockingFuture<>(this).future(defaultValue);
    }
    
    public final CompletableFuture<T> toCompletableFuture() {
        return new BlockingFuture<>(this).completableFuture();
    }
    
    public final CompletableFuture<T> toCompletableFuture(T defaultValue) {
        return new BlockingFuture<>(this).completableFuture(defaultValue);
    }
    
    public final <U> Px<List<T>> buffer(Publisher<U> other, int maxSize) {
        return onAssembly(new PublisherBufferBoundaryAndSize<>(this, other, () -> new ArrayList<>(), maxSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    public final <U, C extends Collection<? super T>> Px<C> buffer(Publisher<U> other, int maxSize, Supplier<C> bufferSupplier) {
        return onAssembly(new PublisherBufferBoundaryAndSize<>(this, other, bufferSupplier, maxSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)));
    }

    @Override
    public int getMode() {
        return FACTORY;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    public final <R> Px<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return flatMap(mapper, false, Integer.MAX_VALUE, BUFFER_SIZE);
    }

    public final <R> Px<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError) {
        return flatMap(mapper, delayError, Integer.MAX_VALUE, BUFFER_SIZE);
    }

    public final <R> Px<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency) {
        return flatMap(mapper, delayError, maxConcurrency, BUFFER_SIZE);
    }

    public final <R> Px<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency, int prefetch) {
        return onAssembly(new PublisherFlatMap<>(this, mapper, delayError, maxConcurrency, defaultQueueSupplier(maxConcurrency), prefetch, defaultQueueSupplier(prefetch)));
    }

    @SuppressWarnings("unchecked")
    public final <U, R> Px<R> zipWith(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        if (this instanceof PublisherZip) {
            PublisherZip<T, R> o = (PublisherZip<T, R>) this;
            Px<R> result = o.zipAdditionalSource(other, zipper);
            if (result != null) {
                return onAssembly(result);
            }
        }
        Px<R> result = new PublisherZip<>(this, other, zipper, defaultQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
        return onAssembly(result);
    }
    
    /**
     * Hides the identity of this Publisher, including its Subscription type.
     * <p>
     * This operator is aimed at preventing certain operator optimizations such
     * as operator macro- and micro-fusion.
     * 
     * @return the new Px hiding this Publisher
     */
    public final Px<T> hide() {
        return onAssembly(new PublisherHide<>(this));
    }
    
    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return concatMap(mapper, PublisherConcatMap.ErrorMode.IMMEDIATE, BUFFER_SIZE);
    }
    
    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode) {
        return concatMap(mapper, errorMode, BUFFER_SIZE);
    }

    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode, int prefetch) {
        return onAssembly(new PublisherConcatMap<>(this, mapper, defaultUnboundedQueueSupplier(prefetch), prefetch, errorMode));
    }

    public final Px<T> observeOn(ExecutorService executor) {
        return observeOn(executor, true, BUFFER_SIZE);
    }

    public final Px<T> observeOn(ExecutorService executor, boolean delayError) {
        return observeOn(executor, delayError, BUFFER_SIZE);
    }
    
    public final Px<T> observeOn(ExecutorService executor, boolean delayError, int prefetch) {
        if (this instanceof Fuseable.ScalarCallable) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarCallable<T>)this).call();
            return onAssembly(new PublisherSubscribeOnValue<>(value, fromExecutor(executor)));
        }
        return onAssembly(new PublisherObserveOn<>(this, fromExecutor(executor), delayError, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final Px<T> observeOn(Scheduler scheduler) {
        return observeOn(scheduler, true, BUFFER_SIZE);
    }

    public final Px<T> observeOn(Scheduler scheduler, boolean delayError) {
        return observeOn(scheduler, delayError, BUFFER_SIZE);
    }
    
    public final Px<T> observeOn(Scheduler scheduler, boolean delayError, int prefetch) {
        if (this instanceof Fuseable.ScalarCallable) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarCallable<T>)this).call();
            return onAssembly(new PublisherSubscribeOnValue<>(value, scheduler));
        }
        return onAssembly(new PublisherObserveOn<>(this, scheduler, delayError, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final Px<T> subscribeOn(ExecutorService executor) {
        Scheduler fromExecutor = fromExecutor(executor);
        return subscribeOn(fromExecutor);
    }

    @SuppressWarnings("unchecked")
    public final Px<T> subscribeOn(Scheduler scheduler) {
        if (this instanceof Callable) {
            if (this instanceof Fuseable.ScalarCallable) {
                T value = ((Fuseable.ScalarCallable<T>)this).call();
                return onAssembly(new PublisherSubscribeOnValue<>(value, scheduler));
            }
            return onAssembly(new PublisherCallableSubscribeOn<>((Callable<T>)this, scheduler));
        }
        return onAssembly(new PublisherSubscribeOn<>(this, scheduler));
    }

    public final Px<T> aggregate(BiFunction<T, T, T> aggregator) {
        if (this instanceof Callable) {
            return onAssembly(this);
        }
        return onAssembly(new PublisherAggregate<>(this, aggregator));
    }

    public final Px<T> reduce(BiFunction<T, T, T> aggregator) {
        return aggregate(aggregator);
    }
    
    public final ConnectablePublisher<T> publish() {
        return publish(BUFFER_SIZE);
    }
    
    public final ConnectablePublisher<T> publish(int prefetch) {
        return onAssembly(new ConnectablePublisherPublish<>(this, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final <K> Px<GroupedPublisher<K, T>> groupBy(Function<? super T, ? extends K> keySelector) {
        return groupBy(keySelector, v -> v);
    }

    public final <K, V> Px<GroupedPublisher<K, V>> groupBy(Function<? super T, ? extends K> keySelector, Function<? super T, ? extends V> valueSelector) {
        return onAssembly(new PublisherGroupBy<>(this, keySelector, valueSelector, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE));
    }

    public final <U> Px<Px<T>> windowBatch(int maxSize, Supplier<? extends Publisher<U>> boundarySupplier) {
        return onAssembly(new PublisherWindowBatch<>(this, boundarySupplier, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize));
    }

    public final ConnectablePublisher<T> process(Processor<? super T, ? extends T> processor) {
        return process(() -> processor);
    }

    @SuppressWarnings("unchecked")
    public final ConnectablePublisher<T> process(
            Supplier<? extends Processor<? super T, ? extends T>> processorSupplier) {
        return process(processorSupplier, IDENTITY_FUNCTION);
    }

    public final <U> ConnectablePublisher<U> process(Processor<? super T, ? extends T>
            processor, Function<Px<T>, ? extends Publisher<? extends U>> selector) {
        return process(() -> processor, selector);
    }

    public final <U> ConnectablePublisher<U> process(Supplier<? extends Processor<? super T, ? extends T>>
            processorSupplier, Function<Px<T>, ? extends Publisher<? extends U>> selector) {
        return onAssembly(new ConnectablePublisherProcess<>(this, processorSupplier, selector));
    }
    
    public final Px<T> onTerminateDetach() {
        return onAssembly(new PublisherDetach<>(this));
    }
    
    public final Px<T> awaitOnSubscribe() {
        return onAssembly(new PublisherAwaitOnSubscribe<>(this));
    }
    
    public final Px<T> mergeWith(Publisher<? extends T> other) {
        if (this instanceof PublisherMerge) {
            return onAssembly(((PublisherMerge<T>)this).mergeAdditionalSource(other, Px::defaultQueueSupplier));
        }
        return mergeArray(this, other);
    }
    
    public final <R> Px<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        return concatMapIterable(mapper, BUFFER_SIZE);
    }

    public final <R> Px<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        return onAssembly(new PublisherFlattenIterable<>(this, mapper, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final <R> Px<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        return flatMapIterable(mapper, BUFFER_SIZE);
    }

    public final <R> Px<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        return onAssembly(new PublisherFlattenIterable<>(this, mapper, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final <R, A> Px<R> collect(Collector<T, A, R> collector) {
        return onAssembly(new PublisherStreamCollector<>(this, collector));
    }

    public final <R> Px<R> publish(Function<? super Px<T>, ? extends Publisher<? extends R>> transform) {
        return publish(transform, BUFFER_SIZE);
    }
    
    public final <R> Px<R> publish(Function<? super Px<T>, ? extends Publisher<? extends R>> transform, int prefetch) {
        return onAssembly(new PublisherPublish<>(this, transform, prefetch, defaultQueueSupplier(prefetch)));
    }

    public final ParallelPublisher<T> parallel() {
        return ParallelPublisher.from(this);
    }

    public final ParallelPublisher<T> parallel(boolean ordered) {
        return ParallelPublisher.from(this, ordered);
    }

    public final ParallelPublisher<T> parallel(int parallelism) {
        return ParallelPublisher.from(this, false, parallelism);
    }

    public final ParallelPublisher<T> parallel(boolean ordered, int parallelism) {
        return ParallelPublisher.from(this, ordered, parallelism);
    }

    @SuppressWarnings("unchecked")
    public final Px<Integer> sumInt() {
        return new PublisherSumInt((Px<Integer>)this);
    }
    
    @SuppressWarnings("unchecked")
    public final Px<Long> sumLong() {
        return new PublisherSumLong((Px<Long>)this);
    }

    @SuppressWarnings("unchecked")
    public final Px<Integer> minInt() {
        return new PublisherMinInt((Px<Integer>)this);
    }

    @SuppressWarnings("unchecked")
    public final Px<Integer> maxInt() {
        return new PublisherMaxInt((Px<Integer>)this);
    }
    
    // ------------------------------------------------------------------------------------------------
    
    public final Cancellation subscribe() {
        EmptyAsyncSubscriber<T> s = new EmptyAsyncSubscriber<>();
        subscribe(s);
        return s;
    }
    
    public final Cancellation subscribe(Consumer<? super T> onNext) {
        return subscribe(onNext, DROP_ERROR, EMPTY_RUNNABLE);
    }

    public final Cancellation subscribe(Consumer<? super T> onNext, Consumer<Throwable> onError) {
        return subscribe(onNext, onError, EMPTY_RUNNABLE);
    }

    public final Cancellation subscribe(Consumer<? super T> onNext, Consumer<Throwable> onError, Runnable onComplete) {
        LambdaSubscriber<T> s = new LambdaSubscriber<>(onNext, onError, onComplete);
        subscribe(s);
        return s;
    }

    public TestSubscriber<T> test() {
        TestSubscriber<T> ts = new TestSubscriber<>();
        subscribe(ts);
        return ts;
    }

    public TestSubscriber<T> test(long initialRequest) {
        TestSubscriber<T> ts = new TestSubscriber<>(initialRequest);
        subscribe(ts);
        return ts;
    }

    public TestSubscriber<T> test(long initialRequest, int fusionMode) {
        TestSubscriber<T> ts = new TestSubscriber<>(initialRequest);
        ts.requestedFusionMode(fusionMode);
        subscribe(ts);
        return ts;
    }

    public TestSubscriber<T> test(long initialRequest, int fusionMode, boolean cancelled) {
        TestSubscriber<T> ts = new TestSubscriber<>(initialRequest);
        ts.requestedFusionMode(fusionMode);
        if (cancelled) {
            ts.cancel();
        }
        subscribe(ts);
        return ts;
    }

    public TestSubscriber<T> testFusion(int fusionMode) {
        TestSubscriber<T> ts = new TestSubscriber<>();
        ts.requestedFusionMode(fusionMode);
        subscribe(ts);
        return ts;
    }
    
    public final T peekLast() {
        PeekLastSubscriber<T> subscriber = new PeekLastSubscriber<>();
        subscribe(subscriber);
        return subscriber.get();
    }
    
    public final T blockingFirst() {
        BlockingFirstSubscriber<T> subscriber = new BlockingFirstSubscriber<>();
        subscribe(subscriber);
        return subscriber.blockingGet();
    }

    public final T blockingFirst(long timeout, TimeUnit unit) {
        BlockingFirstSubscriber<T> subscriber = new BlockingFirstSubscriber<>();
        subscribe(subscriber);
        return subscriber.blockingGet(timeout, unit);
    }

    public final T blockingLast() {
        BlockingLastSubscriber<T> subscriber = new BlockingLastSubscriber<>();
        subscribe(subscriber);
        return subscriber.blockingGet();
    }

    public final T blockingLast(long timeout, TimeUnit unit) {
        BlockingLastSubscriber<T> subscriber = new BlockingLastSubscriber<>();
        subscribe(subscriber);
        return subscriber.blockingGet(timeout, unit);
    }
    
    // ---------------------------------------------------------------------------------------
    
    public static <T> Px<T> from(Publisher<? extends T> source) {
        return wrap(source);
    }

    @SuppressWarnings("unchecked")
    public static <T> Px<T> wrap(Publisher<? extends T> source) {
        if (source instanceof Px) {
            return onAssembly((Px<T>)source);
        }
        return onAssembly(new PxWrapper<>(source));
    }

    /**
     * Wraps an arbitrary publisher source into a Px instance which also
     * implements Fuseable indicating the source publisher can deal with the
     * operator fusion API internally.
     * 
     * @param <T> the value type
     * @param source the publisher to wrap
     * @return the Px instance
     */
    @SuppressWarnings("unchecked")
    public static <T> Px<T> wrapFuseable(Publisher<? extends T> source) {
        if (source instanceof Px) {
            return onAssembly((Px<T>)source);
        }
        return onAssembly(new PxFuseableWrapper<>(source));
    }

    // ---------------------------------------------------------------------------------------
    
    public static <T> Px<T> just(T value) {
        return onAssembly(new PublisherJust<>(value));
    }
    
    public static <T> Px<T> empty() {
        return onAssembly(PublisherEmpty.instance());
    }

    public static <T> Px<T> never() {
        return onAssembly(PublisherNever.instance());
    }
    
    public static <T> Px<T> error(Throwable error) {
        return onAssembly(new PublisherError<>(error));
    }

    public static <T> Px<T> error(Throwable error, boolean whenRequested) {
        return onAssembly(new PublisherError<>(error, whenRequested));
    }

    public static <T> Px<T> error(Supplier<? extends Throwable> errorSupplier) {
        return onAssembly(new PublisherError<>(errorSupplier, false));
    }

    public static <T> Px<T> error(Supplier<? extends Throwable> errorSupplier, boolean whenRequested) {
        return onAssembly(new PublisherError<>(errorSupplier, whenRequested));
    }

    public static Px<Integer> range(int start, int count) {
        if (count == 0) {
            return empty();
        }
        if (count == 1) {
            return just(start);
        }
        return onAssembly(new PublisherRange(start, count));
    }
    
    @SafeVarargs
    public static <T> Px<T> fromArray(T... array) {
        int n = array.length;
        if (n == 0) {
            return empty();
        } else
        if (n == 1) {
            return just(array[0]);
        }
        return onAssembly(new PublisherArray<>(array));
    }

    public static <T> Px<T> fromIterable(Iterable<? extends T> iterable) {
        return onAssembly(new PublisherIterable<>(iterable));
    }

    public static <T> Px<T> fromCallable(Callable<? extends T> callable) {
        return onAssembly(new PublisherCallable<>(callable));
    }
    
    @SafeVarargs
    public static <T, R> Px<R> zip(Function<? super Object[], ? extends R> zipper, Publisher<? extends T>... sources) {
        return onAssembly(new PublisherZip<>(sources, zipper, defaultQueueSupplier(BUFFER_SIZE), BUFFER_SIZE));
    }

    @SafeVarargs
    public static <T, R> Px<R> zip(Function<? super Object[], ? extends R> zipper, int prefetch, Publisher<? extends T>... sources) {
        return onAssembly(new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch));
    }

    public static <T, R> Px<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper) {
        return zipArray(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> Px<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper) {
        return zipIterable(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> Px<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return onAssembly(new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch));
    }

    public static <T, R> Px<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return onAssembly(new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch));
    }
    
    @SafeVarargs
    public static <T> Px<T> concatArray(Publisher<? extends T>... sources) {
        return concatArray(false, sources);
    }

    @SafeVarargs
    public static <T> Px<T> concatArray(boolean delayError, Publisher<? extends T>... sources) {
        return onAssembly(new PublisherConcatArray<>(delayError, sources));
    }

    public static <T> Px<T> concatIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return onAssembly(new PublisherConcatIterable<>(sources));
    }

    @SafeVarargs
    public static <T> Px<T> mergeArray(Publisher<? extends T>... sources) {
        return onAssembly(new PublisherMerge<>(sources, false, Integer.MAX_VALUE, defaultQueueSupplier(Integer.MAX_VALUE), BUFFER_SIZE, defaultQueueSupplier(BUFFER_SIZE)));
    }

    @SuppressWarnings("unchecked")
    public static <T> Px<T> mergeIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return fromIterable(sources).flatMap(IDENTITY_FUNCTION);
    }

    public static Px<Long> timer(long delay, TimeUnit unit, TimedScheduler executor) {
        return onAssembly(new PublisherTimer(delay, unit, executor));
    }

    public static Px<Long> interval(long period, TimeUnit unit, TimedScheduler executor) {
        return interval(period, period, unit, executor);
    }

    public static Px<Long> interval(long initialDelay,long period, TimeUnit unit, TimedScheduler executor) {
        return onAssembly(new PublisherInterval(initialDelay, period, unit, executor));
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Px<R> combineLatest(Publisher<? extends T> p1, Publisher<? extends U> p2, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return onAssembly(new PublisherCombineLatest<T, R>(new Publisher[] { p1, p2 }, a -> combiner.apply((T)a[0], (U)a[1]),
                defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE));
    }
    
    public static <T, S> Px<T> using(Callable<S> resourceSupplier, 
            Function<? super S, ? extends Publisher<? extends T>> sourceCreator, Consumer<? super S> disposer) {
        return using(resourceSupplier, sourceCreator, disposer, true);
    }

    public static <T, S> Px<T> using(Callable<S> resourceSupplier, 
            Function<? super S, ? extends Publisher<? extends T>> sourceCreator, Consumer<? super S> disposer,
                    boolean eager) {
        return onAssembly(new PublisherUsing<>(resourceSupplier, sourceCreator, disposer, eager));
    }

    public static <T> Px<T> defer(Supplier<? extends Px<? extends T>> callback) {
        return onAssembly(new PublisherDefer<>(callback));
    }

    public static <T, S> Px<T> generate(BiFunction<S, SignalEmitter<T>, S> generator) {
        return onAssembly(new PublisherGenerate<>(generator));
    }

    public static <T, S> Px<T> generate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator) {
        return onAssembly(new PublisherGenerate<>(stateSupplier, generator));
    }

    public static <T, S> Px<T> generate(Callable<S> stateSupplier, BiFunction<S, SignalEmitter<T>, S> generator, Consumer<? super S> stateConsumer) {
        return onAssembly(new PublisherGenerate<>(stateSupplier, generator, stateConsumer));
    }

    public static Px<Integer> characters(CharSequence string) {
        return onAssembly(new PublisherCharSequence(string));
    }

    @SuppressWarnings("rawtypes")
    static final Function IDENTITY_FUNCTION = new Function() {
        @Override
        public Object apply(Object t) {
            return t;
        }
    };
    
    static final Runnable EMPTY_RUNNABLE = new Runnable() {
        @Override
        public void run() { 
            
        }
        
        @Override
        public String toString() {
            return "EMPTY_RUNNABLE";
        }
    };
    
    static final Consumer<Throwable> DROP_ERROR = new Consumer<Throwable>() {
        @Override
        public void accept(Throwable t) {
            UnsignalledExceptions.onErrorDropped(t);
        }
        
        @Override
        public String toString() {
            return "UnsignalledExceptions::onErrorDropped";
        }
    };
    
    static Scheduler fromExecutor(ExecutorService executor) {
        Objects.requireNonNull(executor, "executor");
        return new ExecutorServiceScheduler(executor);
    }
}
