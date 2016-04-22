package rsc.publisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

import org.reactivestreams.*;

import rsc.flow.*;
import rsc.scheduler.*;
import rsc.state.Introspectable;
import rsc.subscriber.*;
import rsc.util.*;

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
    
    static final Supplier<Queue<Object>> QUEUE_SUPPLIER = new Supplier<Queue<Object>>() {
        @Override
        public Queue<Object> get() {
            return new ConcurrentLinkedQueue<>();
        }
    };
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static <T> Supplier<Queue<T>> defaultQueueSupplier(final int capacity) {
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
    static <T> Supplier<Queue<T>> defaultUnboundedQueueSupplier(final int capacity) {
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

    public final <R> Px<R> map(Function<? super T, ? extends R> mapper) {
        if (this instanceof Fuseable) {
            return new PublisherMapFuseable<>(this, mapper);
        }
        return new PublisherMap<>(this, mapper);
    }
    
    public final Px<T> filter(Predicate<? super T> predicate) {
        if (this instanceof Fuseable) {
            return new PublisherFilterFuseable<>(this, predicate);
        }
        return new PublisherFilter<>(this, predicate);
    }
    
    public final Px<T> take(long n) {
        return new PublisherTake<>(this, n);
    }
    
    public final Px<T> concatWith(Publisher<? extends T> other) {
        return new PublisherConcatArray<>(false, this, other);
    }
    
    public final Px<T> ambWith(Publisher<? extends T> other) {
        return new PublisherAmb<>(this, other);
    }
    
    public final <U, R> Px<R> withLatestFrom(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return new PublisherWithLatestFrom<>(this, other, combiner);
    }
    
    public final <R> Px<R> switchMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return new PublisherSwitchMap<>(this, mapper, defaultQueueSupplier(Integer.MAX_VALUE), BUFFER_SIZE);
    }
    
    public final Px<T> retryWhen(Function<? super Px<Throwable>, ? extends Publisher<? extends Object>> whenFunction) {
        return new PublisherRetryWhen<>(this, whenFunction);
    }

    public final Px<T> repeatWhen(Function<? super Px<Long>, ? extends Publisher<? extends
            Object>> whenFunction) {
        return new PublisherRepeatWhen<>(this, whenFunction);
    }

    public final <U> Px<List<T>> buffer(Publisher<U> other) {
        return buffer(other, () -> new ArrayList<>());
    }
    
    public final <U, C extends Collection<? super T>> Px<C> buffer(Publisher<U> other, Supplier<C> bufferSupplier) {
        return new PublisherBufferBoundary<>(this, other, bufferSupplier);
    }

    public final <U, V> Px<List<T>> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return buffer(start, end, () -> new ArrayList<>());
    }

    public final <U, V, C extends Collection<? super T>> Px<C> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end, 
                    Supplier<C> bufferSupplier) {
        return new PublisherBufferStartEnd<>(this, start, end, bufferSupplier, defaultQueueSupplier(Integer.MAX_VALUE));
    }

    public final <U> Px<Px<T>> window(Publisher<U> other) {
        return new PublisherWindowBoundary<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U> Px<Px<T>> window(Publisher<U> other, int maxSize) {
        return new PublisherWindowBoundaryAndSize<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize);
    }

    public final <U> Px<Px<T>> window(Publisher<U> other, int maxSize, boolean allowEmptyWindows) {
        if (!allowEmptyWindows) {
            return new PublisherWindowBoundaryAndSizeNonEmpty<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize);
        }
        return new PublisherWindowBoundaryAndSize<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize);
    }

    public final Px<T> accumulate(BiFunction<T, ? super T, T> accumulator) {
        return new PublisherAccumulate<>(this, accumulator);
    }
    
    public final Px<Boolean> all(Predicate<? super T> predicate) {
        return new PublisherAll<>(this, predicate);
    }
    
    public final Px<Boolean> any(Predicate<? super T> predicate) {
        return new PublisherAny<>(this, predicate);
    }
    
    public final Px<Boolean> exists(T value) {
        return any(v -> Objects.equals(v, value));
    }
    
    public final Px<List<T>> buffer(int count) {
        return new PublisherBuffer<>(this, count, () -> new ArrayList<>());
    }
    
    public final Px<List<T>> buffer(int count, int skip) {
        return new PublisherBuffer<>(this, count, skip, () -> new ArrayList<>());
    }
    
    public final <C extends Collection<? super T>> Px<C> buffer(int count, int skip, Supplier<C> bufferSupplier) {
        return new PublisherBuffer<>(this, count, skip, bufferSupplier);
    }

    public final <R> Px<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> collector) {
        return new PublisherCollect<>(this, supplier, collector);
    }
    
    public final Px<List<T>> toList() {
        return collect(() -> new ArrayList<>(), (a, b) -> a.add(b));
    }
    
    public final Px<Long> count() {
        return new PublisherCount<>(this);
    }
    
    public final Px<T> defaultIfEmpty(T value) {
        return new PublisherDefaultIfEmpty<>(this, value);
    }
    
    public final <U> Px<T> delaySubscription(Publisher<U> other) {
        return new PublisherDelaySubscription<>(this, other);
    }
    
    public final Px<T> distinct() {
        return distinct(v -> v);
    }
    
    public final <K> Px<T> distinct(Function<? super T, K> keyExtractor) {
        return new PublisherDistinct<>(this, keyExtractor, () -> new HashSet<>());
    }
    
    public final Px<T> distinctUntilChanged() {
        return distinctUntilChanged(v -> v);
    }
    
    public final <K> Px<T> distinctUntilChanged(Function<? super T, K> keyExtractor) {
        return new PublisherDistinctUntilChanged<>(this, keyExtractor);
    }
    
    public final Px<T> onBackpressureDrop() {
        return new PublisherDrop<>(this);
    }
    
    public final Px<T> elementAt(long index) {
        return new PublisherElementAt<>(this, index);
    }
    
    public final Px<T> elementAt(long index, T defaultValue) {
        return new PublisherElementAt<>(this, index, () -> defaultValue);
    }
    
    public final Px<T> ignoreElements() {
        return new PublisherIgnoreElements<>(this);
    }
    
    public final Px<T> onBackpressureLatest() {
        return new PublisherLatest<>(this);
    }
    
    public final <R> Px<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> onLift) {
        return new PublisherLift<>(this, onLift);
    }
    
    public final Px<T> next() {
        return new PublisherNext<>(this);
    }

    public final Px<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, onSubscribe, null, null, null, null, null, null);
        }
        return new PublisherPeek<>(this, onSubscribe, null, null, null, null, null, null);
    }
    
    public final Px<T> doOnNext(Consumer<? super T> onNext) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, onNext, null, null, null, null, null);
        }
        return new PublisherPeek<>(this, null, onNext, null, null, null, null, null);
    }

    public final Px<T> doOnError(Consumer<? super Throwable> onError) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, onError, null, null, null, null);
        }
        return new PublisherPeek<>(this, null, null, onError, null, null, null, null);
    }

    public final Px<T> doOnComplete(Runnable onComplete) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, onComplete, null, null, null);
        }
        return new PublisherPeek<>(this, null, null, null, onComplete, null, null, null);
    }
    
    public final Px<T> doAfterTerminate(Runnable onAfterTerminate) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, onAfterTerminate, null, null);
        }
        return new PublisherPeek<>(this, null, null, null, null, onAfterTerminate, null, null);
    }

    public final Px<T> doOnRequest(LongConsumer onRequest) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, null, onRequest, null);
        }
        return new PublisherPeek<>(this, null, null, null, null, null, onRequest, null);
    }
    
    public final Px<T> doOnCancel(Runnable onCancel) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, null, null, onCancel);
        }
        return new PublisherPeek<>(this, null, null, null, null, null, null, onCancel);
    }

    public final <R> Px<R> reduce(Supplier<R> initialValue, BiFunction<R, ? super T, R> accumulator) {
        return new PublisherReduce<>(this, initialValue, accumulator);
    }
    
    public final Px<T> repeat() {
        return new PublisherRepeat<>(this);
    }

    public final Px<T> repeat(long times) {
        return new PublisherRepeat<>(this, times);
    }

    public final Px<T> repeat(BooleanSupplier predicate) {
        return new PublisherRepeatPredicate<>(this, predicate);
    }

    public final Px<T> retry() {
        return new PublisherRetry<>(this);
    }

    public final Px<T> retry(long times) {
        return new PublisherRetry<>(this, times);
    }

    public final Px<T> retry(Predicate<Throwable> predicate) {
        return new PublisherRetryPredicate<>(this, predicate);
    }
    
    public final Px<T> onErrorReturn(T value) {
        return onErrorResumeNext(new PublisherJust<>(value));
    }
    
    public final Px<T> onErrorResumeNext(Publisher<? extends T> next) {
        return onErrorResumeNext(e -> next);
    }
    
    public final Px<T> onErrorResumeNext(Function<Throwable, ? extends Publisher<? extends T>> nextFunction) {
        return new PublisherResume<>(this, nextFunction);
    }
    
    public final <U> Px<T> sample(Publisher<U> sampler) {
        return new PublisherSample<>(this, sampler);
    }
    
    public final <R> Px<R> scan(R initialValue, BiFunction<R, ? super T, R> accumulator) {
        return new PublisherScan<>(this, initialValue, accumulator);
    }
    
    public final Px<T> single() {
        return new PublisherSingle<>(this);
    }

    public final Px<T> single(T defaultValue) {
        return new PublisherSingle<>(this, () -> defaultValue);
    }

    public final Px<T> skip(long n) {
        return new PublisherSkip<>(this, n);
    }
    
    public final Px<T> skipLast(int n) {
        return new PublisherSkipLast<>(this, n);
    }
    
    public final Px<T> skipWhile(Predicate<? super T> predicate) {
        return new PublisherSkipWhile<>(this, predicate);
    }

    public final <U> Px<T> skipUntil(Publisher<U> other) {
        return new PublisherSkipUntil<>(this, other);
    }
    
    public final Px<T> switchIfEmpty(Publisher<? extends T> other) {
        return new PublisherSwitchIfEmpty<>(this, other);
    }
    
    public final <U, V> Px<Px<T>> window(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return new PublisherWindowStartEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U, V> Px<Px<T>> window2(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return new PublisherWindowBeginEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }

    public final Px<T> takeLast(int n) {
        return new PublisherTakeLast<>(this, n);
    }
    
    public final <U> Px<T> takeUntil(Publisher<U> other) {
        return new PublisherTakeUntil<>(this, other);
    }
    
    public final Px<T> takeUntil(Predicate<? super T> predicate) {
        return new PublisherTakeUntilPredicate<>(this, predicate);
    }

    public final Px<T> takeWhile(Predicate<? super T> predicate) {
        return new PublisherTakeWhile<>(this, predicate);
    }
    
    public final <U, V> Px<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout) {
        return new PublisherTimeout<>(this, firstTimeout, itemTimeout);
    }

    public final <U, V> Px<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout, Publisher<? extends T> other) {
        return new PublisherTimeout<>(this, firstTimeout, itemTimeout, other);
    }

    public final <U, R> Px<R> zipWith(Iterable<U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        return new PublisherZipIterable<>(this, other, zipper);
    }
    
    public final <U> Px<T> throttleFirst(Function<? super T, ? extends Publisher<U>> throttler) {
        return new PublisherThrottleFirst<>(this, throttler);
    }
    
    public final <U> Px<T> throttleLast(Publisher<U> throttler) {
        return sample(throttler);
    }
    
    public final <U> Px<T> throttleTimeout(Function<? super T, ? extends Publisher<U>> throttler) {
        return new PublisherThrottleTimeout<>(this, throttler, defaultUnboundedQueueSupplier(BUFFER_SIZE));
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
        return new PublisherBufferBoundaryAndSize<>(this, other, () -> new ArrayList<>(), maxSize, defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U, C extends Collection<? super T>> Px<C> buffer(Publisher<U> other, int maxSize, Supplier<C> bufferSupplier) {
        return new PublisherBufferBoundaryAndSize<>(this, other, bufferSupplier, maxSize, defaultUnboundedQueueSupplier(BUFFER_SIZE));
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
        return new PublisherFlatMap<>(this, mapper, delayError, maxConcurrency, defaultQueueSupplier(maxConcurrency), prefetch, defaultQueueSupplier(prefetch));
    }

    @SuppressWarnings("unchecked")
    public final <U, R> Px<R> zipWith(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        return zipArray(new Publisher[] { this, other }, a -> {
            return zipper.apply((T)a[0], (U)a[1]);
        });
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
        return new PublisherHide<>(this);
    }
    
    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return concatMap(mapper, PublisherConcatMap.ErrorMode.IMMEDIATE, BUFFER_SIZE);
    }
    
    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode) {
        return concatMap(mapper, errorMode, BUFFER_SIZE);
    }

    public final <R> Px<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode, int prefetch) {
        return new PublisherConcatMap<>(this, mapper, defaultUnboundedQueueSupplier(prefetch), prefetch, errorMode);
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
            return new PublisherSubscribeOnValue<>(value, fromExecutor(executor));
        }
        return new PublisherObserveOn<>(this, fromExecutor(executor), delayError, prefetch, defaultQueueSupplier(prefetch));
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
            return new PublisherSubscribeOnValue<>(value, scheduler);
        }
        return new PublisherObserveOn<>(this, scheduler, delayError, prefetch, defaultQueueSupplier(prefetch));
    }

    public final Px<T> subscribeOn(ExecutorService executor) {
        if (this instanceof Fuseable.ScalarCallable) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarCallable<T>)this).call();
            return new PublisherSubscribeOnValue<>(value, fromExecutor(executor));
        }
        return new PublisherSubscribeOn<>(this, fromExecutor(executor));
    }

    public final Px<T> subscribeOn(Scheduler scheduler) {
        if (this instanceof Fuseable.ScalarCallable) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarCallable<T>)this).call();
            return new PublisherSubscribeOnValue<>(value, scheduler);
        }
        return new PublisherSubscribeOn<>(this, scheduler);
    }

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

    public final Px<T> aggregate(BiFunction<T, T, T> aggregator) {
        if (this instanceof Callable) {
            return this;
        }
        return new PublisherAggregate<>(this, aggregator);
    }

    public final Px<T> reduce(BiFunction<T, T, T> aggregator) {
        return aggregate(aggregator);
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
    
    public final ConnectablePublisher<T> publish() {
        return publish(BUFFER_SIZE);
    }
    
    public final ConnectablePublisher<T> publish(int prefetch) {
        return new ConnectablePublisherPublish<>(this, prefetch, defaultQueueSupplier(prefetch));
    }

    public final <K> Px<GroupedPublisher<K, T>> groupBy(Function<? super T, ? extends K> keySelector) {
        return groupBy(keySelector, v -> v);
    }

    public final <K, V> Px<GroupedPublisher<K, V>> groupBy(Function<? super T, ? extends K> keySelector, Function<? super T, ? extends V> valueSelector) {
        return new PublisherGroupBy<>(this, keySelector, valueSelector, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }

    public final <U> Px<Px<T>> windowBatch(int maxSize, Supplier<? extends Publisher<U>> boundarySupplier) {
        return new PublisherWindowBatch<>(this, boundarySupplier, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize);
    }

    public final ConnectablePublisher<T> multicast(Processor<? super T, ? extends T> processor) {
        return multicast(() -> processor);
    }

    @SuppressWarnings("unchecked")
    public final ConnectablePublisher<T> multicast(
            Supplier<? extends Processor<? super T, ? extends T>> processorSupplier) {
        return multicast(processorSupplier, IDENTITY_FUNCTION);
    }

    public final <U> ConnectablePublisher<U> multicast(Processor<? super T, ? extends T>
            processor, Function<Px<T>, ? extends Publisher<? extends U>> selector) {
        return multicast(() -> processor, selector);
    }

    public final <U> ConnectablePublisher<U> multicast(Supplier<? extends Processor<? super T, ? extends T>>
            processorSupplier, Function<Px<T>, ? extends Publisher<? extends U>> selector) {
        return new ConnectablePublisherMulticast<>(this, processorSupplier, selector);
    }
    
    public final Px<T> onTerminateDetach() {
        return new PublisherDetach<>(this);
    }
    
    public final Px<T> awaitOnSubscribe() {
        return new PublisherAwaitOnSubscribe<>(this);
    }
    
    public final Px<T> mergeWith(Publisher<? extends T> other) {
        if (this instanceof PublisherMerge) {
            return ((PublisherMerge<T>)this).mergeAdditionalSource(other, Px::defaultQueueSupplier);
        }
        return mergeArray(this, other);
    }
    
    public final <R> Px<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        return concatMapIterable(mapper, BUFFER_SIZE);
    }

    public final <R> Px<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        return new PublisherFlattenIterable<>(this, mapper, prefetch, defaultQueueSupplier(prefetch));
    }

    public final <R> Px<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        return flatMapIterable(mapper, BUFFER_SIZE);
    }

    public final <R> Px<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        return new PublisherFlattenIterable<>(this, mapper, prefetch, defaultQueueSupplier(prefetch));
    }

    public final <R, A> Px<R> collect(Collector<T, A, R> collector) {
        return new PublisherStreamCollector<>(this, collector);
    }
    
    // ---------------------------------------------------------------------------------------
    
    static final class PxWrapper<T> extends PublisherSource<T, T> {
        public PxWrapper(Publisher<? extends T> source) {
            super(source);
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            source.subscribe(s);
        }
    }

    static final class PxFuseableWrapper<T> extends PublisherSource<T, T> 
    implements Fuseable {
        public PxFuseableWrapper(Publisher<? extends T> source) {
            super(source);
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            source.subscribe(s);
        }
    }

    public static <T> Px<T> from(Publisher<? extends T> source) {
        return wrap(source);
    }

    @SuppressWarnings("unchecked")
    public static <T> Px<T> wrap(Publisher<? extends T> source) {
        if (source instanceof Px) {
            return (Px<T>)source;
        }
        return new PxWrapper<>(source);
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
            return (Px<T>)source;
        }
        return new PxFuseableWrapper<>(source);
    }

    // ---------------------------------------------------------------------------------------
    
    public static <T> Px<T> just(T value) {
        return new PublisherJust<>(value);
    }
    
    public static <T> Px<T> empty() {
        return PublisherEmpty.instance();
    }

    public static <T> Px<T> never() {
        return PublisherNever.instance();
    }
    
    public static <T> Px<T> error(Throwable error) {
        return new PublisherError<>(error);
    }

    public static <T> Px<T> error(Throwable error, boolean whenRequested) {
        return new PublisherError<>(error, whenRequested);
    }

    public static <T> Px<T> error(Supplier<? extends Throwable> errorSupplier) {
        return new PublisherError<>(errorSupplier, false);
    }

    public static <T> Px<T> error(Supplier<? extends Throwable> errorSupplier, boolean whenRequested) {
        return new PublisherError<>(errorSupplier, whenRequested);
    }

    public static Px<Integer> range(int start, int count) {
        if (count == 0) {
            return empty();
        }
        if (count == 1) {
            return just(start);
        }
        return new PublisherRange(start, count);
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
        return new PublisherArray<>(array);
    }

    public static <T> Px<T> fromIterable(Iterable<? extends T> iterable) {
        return new PublisherIterable<>(iterable);
    }

    public static <T> Px<T> fromCallable(Callable<? extends T> callable) {
        return new PublisherCallable<>(callable);
    }
    
    @SafeVarargs
    public static <T, R> Px<R> zip(Function<? super Object[], ? extends R> zipper, Publisher<? extends T>... sources) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }
    
    public static <T, R> Px<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper) {
        return zipArray(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> Px<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper) {
        return zipIterable(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> Px<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch);
    }

    public static <T, R> Px<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch);
    }
    
    @SafeVarargs
    public static <T> Px<T> concatArray(Publisher<? extends T>... sources) {
        return concatArray(false, sources);
    }

    @SafeVarargs
    public static <T> Px<T> concatArray(boolean delayError, Publisher<? extends T>... sources) {
        return new PublisherConcatArray<>(delayError, sources);
    }

    public static <T> Px<T> concatIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return new PublisherConcatIterable<>(sources);
    }

    @SafeVarargs
    public static <T> Px<T> mergeArray(Publisher<? extends T>... sources) {
        return new PublisherMerge<>(sources, false, Integer.MAX_VALUE, defaultQueueSupplier(Integer.MAX_VALUE), BUFFER_SIZE, defaultQueueSupplier(BUFFER_SIZE));
    }

    @SuppressWarnings("unchecked")
    public static <T> Px<T> mergeIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return fromIterable(sources).flatMap(IDENTITY_FUNCTION);
    }

    public static Px<Long> timer(long delay, TimeUnit unit, TimedScheduler executor) {
        return new PublisherTimer(delay, unit, executor);
    }

    public static Px<Long> interval(long period, TimeUnit unit, TimedScheduler executor) {
        return interval(period, period, unit, executor);
    }

    public static Px<Long> interval(long initialDelay,long period, TimeUnit unit, TimedScheduler executor) {
        return new PublisherInterval(initialDelay, period, unit, executor);
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Px<R> combineLatest(Publisher<? extends T> p1, Publisher<? extends U> p2, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return new PublisherCombineLatest<T, R>(new Publisher[] { p1, p2 }, a -> combiner.apply((T)a[0], (U)a[1]),
                defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }
    
    public static <T, S> Px<T> using(Callable<S> resourceSupplier, 
            Function<? super S, ? extends Publisher<? extends T>> sourceCreator, Consumer<? super S> disposer) {
        return using(resourceSupplier, sourceCreator, disposer, true);
    }

    public static <T, S> Px<T> using(Callable<S> resourceSupplier, 
            Function<? super S, ? extends Publisher<? extends T>> sourceCreator, Consumer<? super S> disposer,
                    boolean eager) {
        return new PublisherUsing<>(resourceSupplier, sourceCreator, disposer, eager);
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
