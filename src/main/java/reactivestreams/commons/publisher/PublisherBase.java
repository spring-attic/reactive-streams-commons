package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Stream;

import org.reactivestreams.*;

import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.state.Introspectable;
import reactivestreams.commons.subscriber.*;
import reactivestreams.commons.util.*;

/**
 * Experimental base class with fluent API.
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
public abstract class PublisherBase<T> implements Publisher<T>, Introspectable {

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

    public final <R> PublisherBase<R> map(Function<? super T, ? extends R> mapper) {
        if (this instanceof Fuseable) {
            return new PublisherMapFuseable<>(this, mapper);
        }
        return new PublisherMap<>(this, mapper);
    }
    
    public final PublisherBase<T> filter(Predicate<? super T> predicate) {
        if (this instanceof Fuseable) {
            return new PublisherFilterFuseable<>(this, predicate);
        }
        return new PublisherFilter<>(this, predicate);
    }
    
    public final PublisherBase<T> take(long n) {
        return new PublisherTake<>(this, n);
    }
    
    public final PublisherBase<T> concatWith(Publisher<? extends T> other) {
        return new PublisherConcatArray<>(this, other);
    }
    
    public final PublisherBase<T> ambWith(Publisher<? extends T> other) {
        return new PublisherAmb<>(this, other);
    }
    
    public final <U, R> PublisherBase<R> withLatestFrom(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return new PublisherWithLatestFrom<>(this, other, combiner);
    }
    
    public final <R> PublisherBase<R> switchMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return new PublisherSwitchMap<>(this, mapper, defaultQueueSupplier(Integer.MAX_VALUE), BUFFER_SIZE);
    }
    
    public final PublisherBase<T> retryWhen(Function<? super PublisherBase<Throwable>, ? extends Publisher<? extends Object>> whenFunction) {
        return new PublisherRetryWhen<>(this, whenFunction);
    }

    public final PublisherBase<T> repeatWhen(Function<? super PublisherBase<Long>, ? extends Publisher<? extends
            Object>> whenFunction) {
        return new PublisherRepeatWhen<>(this, whenFunction);
    }

    public final <U> PublisherBase<List<T>> buffer(Publisher<U> other) {
        return buffer(other, () -> new ArrayList<>());
    }
    
    public final <U, C extends Collection<? super T>> PublisherBase<C> buffer(Publisher<U> other, Supplier<C> bufferSupplier) {
        return new PublisherBufferBoundary<>(this, other, bufferSupplier);
    }

    public final <U, V> PublisherBase<List<T>> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return buffer(start, end, () -> new ArrayList<>());
    }

    public final <U, V, C extends Collection<? super T>> PublisherBase<C> buffer(
            Publisher<U> start, Function<? super U, ? extends Publisher<V>> end, 
                    Supplier<C> bufferSupplier) {
        return new PublisherBufferStartEnd<>(this, start, end, bufferSupplier, defaultQueueSupplier(Integer.MAX_VALUE));
    }

    public final <U> PublisherBase<PublisherBase<T>> window(Publisher<U> other) {
        return new PublisherWindowBoundary<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U> PublisherBase<PublisherBase<T>> window(Publisher<U> other, int maxSize) {
        return new PublisherWindowBoundaryAndSize<>(this, other, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), maxSize);
    }

    public final PublisherBase<T> accumulate(BiFunction<T, ? super T, T> accumulator) {
        return new PublisherAccumulate<>(this, accumulator);
    }
    
    public final PublisherBase<Boolean> all(Predicate<? super T> predicate) {
        return new PublisherAll<>(this, predicate);
    }
    
    public final PublisherBase<Boolean> any(Predicate<? super T> predicate) {
        return new PublisherAny<>(this, predicate);
    }
    
    public final PublisherBase<Boolean> exists(T value) {
        return any(v -> Objects.equals(v, value));
    }
    
    public final PublisherBase<List<T>> buffer(int count) {
        return new PublisherBuffer<>(this, count, () -> new ArrayList<>());
    }
    
    public final PublisherBase<List<T>> buffer(int count, int skip) {
        return new PublisherBuffer<>(this, count, skip, () -> new ArrayList<>());
    }
    
    public final <C extends Collection<? super T>> PublisherBase<C> buffer(int count, int skip, Supplier<C> bufferSupplier) {
        return new PublisherBuffer<>(this, count, skip, bufferSupplier);
    }

    public final <R> PublisherBase<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> collector) {
        return new PublisherCollect<>(this, supplier, collector);
    }
    
    public final PublisherBase<List<T>> toList() {
        return collect(() -> new ArrayList<>(), (a, b) -> a.add(b));
    }
    
    public final PublisherBase<Long> count() {
        return new PublisherCount<>(this);
    }
    
    public final PublisherBase<T> defaultIfEmpty(T value) {
        return new PublisherDefaultIfEmpty<>(this, value);
    }
    
    public final <U> PublisherBase<T> delaySubscription(Publisher<U> other) {
        return new PublisherDelaySubscription<>(this, other);
    }
    
    public final PublisherBase<T> distinct() {
        return distinct(v -> v);
    }
    
    public final <K> PublisherBase<T> distinct(Function<? super T, K> keyExtractor) {
        return new PublisherDistinct<>(this, keyExtractor, () -> new HashSet<>());
    }
    
    public final PublisherBase<T> distinctUntilChanged() {
        return distinctUntilChanged(v -> v);
    }
    
    public final <K> PublisherBase<T> distinctUntilChanged(Function<? super T, K> keyExtractor) {
        return new PublisherDistinctUntilChanged<>(this, keyExtractor);
    }
    
    public final PublisherBase<T> onBackpressureDrop() {
        return new PublisherDrop<>(this);
    }
    
    public final PublisherBase<T> elementAt(long index) {
        return new PublisherElementAt<>(this, index);
    }
    
    public final PublisherBase<T> elementAt(long index, T defaultValue) {
        return new PublisherElementAt<>(this, index, () -> defaultValue);
    }
    
    public final PublisherBase<T> ignoreElements() {
        return new PublisherIgnoreElements<>(this);
    }
    
    public final PublisherBase<T> onBackpressureLatest() {
        return new PublisherLatest<>(this);
    }
    
    public final <R> PublisherBase<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> onLift) {
        return new PublisherLift<>(this, onLift);
    }
    
    public final PublisherBase<T> next() {
        return new PublisherNext<>(this);
    }

    public final PublisherBase<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, onSubscribe, null, null, null, null, null, null);
        }
        return new PublisherPeek<>(this, onSubscribe, null, null, null, null, null, null);
    }
    
    public final PublisherBase<T> doOnNext(Consumer<? super T> onNext) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, onNext, null, null, null, null, null);
        }
        return new PublisherPeek<>(this, null, onNext, null, null, null, null, null);
    }

    public final PublisherBase<T> doOnError(Consumer<? super Throwable> onError) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, onError, null, null, null, null);
        }
        return new PublisherPeek<>(this, null, null, onError, null, null, null, null);
    }

    public final PublisherBase<T> doOnComplete(Runnable onComplete) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, onComplete, null, null, null);
        }
        return new PublisherPeek<>(this, null, null, null, onComplete, null, null, null);
    }
    
    public final PublisherBase<T> doAfterTerminate(Runnable onAfterTerminate) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, onAfterTerminate, null, null);
        }
        return new PublisherPeek<>(this, null, null, null, null, onAfterTerminate, null, null);
    }

    public final PublisherBase<T> doOnRequest(LongConsumer onRequest) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, null, onRequest, null);
        }
        return new PublisherPeek<>(this, null, null, null, null, null, onRequest, null);
    }
    
    public final PublisherBase<T> doOnCancel(Runnable onCancel) {
        if (this instanceof Fuseable) {
            return new PublisherPeekFuseable<>(this, null, null, null, null, null, null, onCancel);
        }
        return new PublisherPeek<>(this, null, null, null, null, null, null, onCancel);
    }

    public final <R> PublisherBase<R> reduce(Supplier<R> initialValue, BiFunction<R, ? super T, R> accumulator) {
        return new PublisherReduce<>(this, initialValue, accumulator);
    }
    
    public final PublisherBase<T> repeat() {
        return new PublisherRepeat<>(this);
    }

    public final PublisherBase<T> repeat(long times) {
        return new PublisherRepeat<>(this, times);
    }

    public final PublisherBase<T> repeat(BooleanSupplier predicate) {
        return new PublisherRepeatPredicate<>(this, predicate);
    }

    public final PublisherBase<T> retry() {
        return new PublisherRetry<>(this);
    }

    public final PublisherBase<T> retry(long times) {
        return new PublisherRetry<>(this, times);
    }

    public final PublisherBase<T> retry(Predicate<Throwable> predicate) {
        return new PublisherRetryPredicate<>(this, predicate);
    }
    
    public final PublisherBase<T> onErrorReturn(T value) {
        return onErrorResumeNext(new PublisherJust<>(value));
    }
    
    public final PublisherBase<T> onErrorResumeNext(Publisher<? extends T> next) {
        return onErrorResumeNext(e -> next);
    }
    
    public final PublisherBase<T> onErrorResumeNext(Function<Throwable, ? extends Publisher<? extends T>> nextFunction) {
        return new PublisherResume<>(this, nextFunction);
    }
    
    public final <U> PublisherBase<T> sample(Publisher<U> sampler) {
        return new PublisherSample<>(this, sampler);
    }
    
    public final <R> PublisherBase<R> scan(R initialValue, BiFunction<R, ? super T, R> accumulator) {
        return new PublisherScan<>(this, initialValue, accumulator);
    }
    
    public final PublisherBase<T> single() {
        return new PublisherSingle<>(this);
    }

    public final PublisherBase<T> single(T defaultValue) {
        return new PublisherSingle<>(this, () -> defaultValue);
    }

    public final PublisherBase<T> skip(long n) {
        return new PublisherSkip<>(this, n);
    }
    
    public final PublisherBase<T> skipLast(int n) {
        return new PublisherSkipLast<>(this, n);
    }
    
    public final PublisherBase<T> skipWhile(Predicate<? super T> predicate) {
        return new PublisherSkipWhile<>(this, predicate);
    }

    public final <U> PublisherBase<T> skipUntil(Publisher<U> other) {
        return new PublisherSkipUntil<>(this, other);
    }
    
    public final PublisherBase<T> switchIfEmpty(Publisher<? extends T> other) {
        return new PublisherSwitchIfEmpty<>(this, other);
    }
    
    public final <U, V> PublisherBase<PublisherBase<T>> window(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return new PublisherWindowStartEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U, V> PublisherBase<PublisherBase<T>> window2(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return new PublisherWindowBeginEnd<>(this, start, end, defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }

    public final PublisherBase<T> takeLast(int n) {
        return new PublisherTakeLast<>(this, n);
    }
    
    public final <U> PublisherBase<T> takeUntil(Publisher<U> other) {
        return new PublisherTakeUntil<>(this, other);
    }
    
    public final PublisherBase<T> takeUntil(Predicate<? super T> predicate) {
        return new PublisherTakeUntilPredicate<>(this, predicate);
    }

    public final PublisherBase<T> takeWhile(Predicate<? super T> predicate) {
        return new PublisherTakeWhile<>(this, predicate);
    }
    
    public final <U, V> PublisherBase<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout) {
        return new PublisherTimeout<>(this, firstTimeout, itemTimeout);
    }

    public final <U, V> PublisherBase<T> timeout(Publisher<U> firstTimeout, Function<? super T, ? extends Publisher<V>> itemTimeout, Publisher<? extends T> other) {
        return new PublisherTimeout<>(this, firstTimeout, itemTimeout, other);
    }

    public final <U, R> PublisherBase<R> zipWith(Iterable<U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        return new PublisherZipIterable<>(this, other, zipper);
    }
    
    public final <U> PublisherBase<T> throttleFirst(Function<? super T, ? extends Publisher<U>> throttler) {
        return new PublisherThrottleFirst<>(this, throttler);
    }
    
    public final <U> PublisherBase<T> throttleLast(Publisher<U> throttler) {
        return sample(throttler);
    }
    
    public final <U> PublisherBase<T> throttleTimeout(Function<? super T, ? extends Publisher<U>> throttler) {
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
        return new BlockingStream<>(this, batchSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)).stream();
    }

    public final Stream<T> parallelStream() {
        return parallelStream(BUFFER_SIZE);
    }
    
    public final Stream<T> parallelStream(long batchSize) {
        return new BlockingStream<>(this, batchSize, defaultUnboundedQueueSupplier(BUFFER_SIZE)).parallelStream();
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
    
    public final <U> PublisherBase<List<T>> buffer(Publisher<U> other, int maxSize) {
        return new PublisherBufferBoundaryAndSize<>(this, other, () -> new ArrayList<>(), maxSize, defaultUnboundedQueueSupplier(BUFFER_SIZE));
    }

    public final <U, C extends Collection<? super T>> PublisherBase<C> buffer(Publisher<U> other, int maxSize, Supplier<C> bufferSupplier) {
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

    public final <R> PublisherBase<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return flatMap(mapper, false, Integer.MAX_VALUE, BUFFER_SIZE);
    }

    public final <R> PublisherBase<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError) {
        return flatMap(mapper, delayError, Integer.MAX_VALUE, BUFFER_SIZE);
    }

    public final <R> PublisherBase<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency) {
        return flatMap(mapper, delayError, maxConcurrency, BUFFER_SIZE);
    }

    public final <R> PublisherBase<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency, int prefetch) {
        return new PublisherFlatMap<>(this, mapper, delayError, maxConcurrency, defaultQueueSupplier(maxConcurrency), prefetch, defaultQueueSupplier(prefetch));
    }

    @SuppressWarnings("unchecked")
    public final <U, R> PublisherBase<R> zipWith(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
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
     * @return the new PublisherBase hiding this Publisher
     */
    public final PublisherBase<T> hide() {
        return new PublisherHide<>(this);
    }
    
    public final <R> PublisherBase<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return concatMap(mapper, PublisherConcatMap.ErrorMode.IMMEDIATE, BUFFER_SIZE);
    }
    
    public final <R> PublisherBase<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode) {
        return concatMap(mapper, errorMode, BUFFER_SIZE);
    }

    public final <R> PublisherBase<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, PublisherConcatMap.ErrorMode errorMode, int prefetch) {
        return new PublisherConcatMap<>(this, mapper, defaultUnboundedQueueSupplier(prefetch), prefetch, errorMode);
    }

    public final PublisherBase<T> observeOn(ExecutorService executor) {
        return observeOn(executor, true, BUFFER_SIZE);
    }

    public final PublisherBase<T> observeOn(ExecutorService executor, boolean delayError) {
        return observeOn(executor, delayError, BUFFER_SIZE);
    }
    
    public final PublisherBase<T> observeOn(ExecutorService executor, boolean delayError, int prefetch) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, fromExecutor(executor), true);
        }
        return new PublisherObserveOn<>(this, fromExecutor(executor), delayError, prefetch, defaultQueueSupplier(prefetch));
    }

    public final PublisherBase<T> observeOn(Callable<? extends Consumer<Runnable>> schedulerFactory) {
        return observeOn(schedulerFactory, true, BUFFER_SIZE);
    }

    public final PublisherBase<T> observeOn(Callable<? extends Consumer<Runnable>> schedulerFactory, boolean delayError) {
        return observeOn(schedulerFactory, delayError, BUFFER_SIZE);
    }
    
    public final PublisherBase<T> observeOn(Callable<? extends Consumer<Runnable>> schedulerFactory, boolean delayError, int prefetch) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, schedulerFactory, true);
        }
        return new PublisherObserveOn<>(this, schedulerFactory, delayError, prefetch, defaultQueueSupplier(prefetch));
    }

    public final PublisherBase<T> subscribeOn(ExecutorService executor) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, fromExecutor(executor), true);
        }
        return new PublisherSubscribeOn<>(this, fromExecutor(executor));
    }

    public final PublisherBase<T> subscribeOn(ExecutorService executor, boolean eagerCancel, boolean requestOn) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, fromExecutor(executor), eagerCancel);
        }
        return new PublisherSubscribeOnOther<>(this, fromExecutor(executor), eagerCancel, requestOn);
    }

    public final PublisherBase<T> subscribeOn(Callable<? extends Consumer<Runnable>> schedulerFactory) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, schedulerFactory, true);
        }
        return new PublisherSubscribeOn<>(this, schedulerFactory);
    }

    public final PublisherBase<T> subscribeOn(Callable<? extends Consumer<Runnable>> schedulerFactory, boolean eagerCancel, boolean requestOn) {
        if (this instanceof Fuseable.ScalarSupplier) {
            @SuppressWarnings("unchecked")
            T value = ((Fuseable.ScalarSupplier<T>)this).get();
            return new PublisherSubscribeOnValue<>(value, schedulerFactory, eagerCancel);
        }
        return new PublisherSubscribeOnOther<>(this, schedulerFactory, eagerCancel, requestOn);
    }
    
    
    public final Runnable subscribe() {
        EmptyAsyncSubscriber<T> s = new EmptyAsyncSubscriber<>();
        subscribe(s);
        return s;
    }
    
    public final PublisherBase<T> aggregate(BiFunction<T, T, T> aggregator) {
        if (this instanceof Supplier) {
            return this;
        }
        return new PublisherAggregate<>(this, aggregator);
    }

    public final PublisherBase<T> reduce(BiFunction<T, T, T> aggregator) {
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

    public final <K> PublisherBase<GroupedPublisher<K, T>> groupBy(Function<? super T, ? extends K> keySelector) {
        return groupBy(keySelector, v -> v);
    }

    public final <K, V> PublisherBase<GroupedPublisher<K, V>> groupBy(Function<? super T, ? extends K> keySelector, Function<? super T, ? extends V> valueSelector) {
        return new PublisherGroupBy<>(this, keySelector, valueSelector, defaultUnboundedQueueSupplier(BUFFER_SIZE), defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }

    // ---------------------------------------------------------------------------------------
    
    static final class PublisherBaseWrapper<T> extends PublisherSource<T, T> {
        public PublisherBaseWrapper(Publisher<? extends T> source) {
            super(source);
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            source.subscribe(s);
        }
    }

    static final class PublisherBaseFuseableWrapper<T> extends PublisherSource<T, T> 
    implements Fuseable {
        public PublisherBaseFuseableWrapper(Publisher<? extends T> source) {
            super(source);
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            source.subscribe(s);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> wrap(Publisher<? extends T> source) {
        if (source instanceof PublisherBase) {
            return (PublisherBase<T>)source;
        }
        return new PublisherBaseWrapper<>(source);
    }

    /**
     * Wraps an arbitrary publisher source into a PublisherBase instance which also
     * implements Fuseable indicating the source publisher can deal with the
     * operator fusion API internally.
     * 
     * @param <T> the value type
     * @param source the publisher to wrap
     * @return the PublisherBase instance
     */
    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> wrapFuseable(Publisher<? extends T> source) {
        if (source instanceof PublisherBase) {
            return (PublisherBase<T>)source;
        }
        return new PublisherBaseFuseableWrapper<>(source);
    }

    // ---------------------------------------------------------------------------------------
    
    public static <T> PublisherBase<T> just(T value) {
        return new PublisherJust<>(value);
    }
    
    public static <T> PublisherBase<T> empty() {
        return PublisherEmpty.instance();
    }

    public static <T> PublisherBase<T> never() {
        return PublisherNever.instance();
    }
    
    public static <T> PublisherBase<T> error(Throwable error) {
        return new PublisherError<>(error);
    }

    public static <T> PublisherBase<T> error(Throwable error, boolean whenRequested) {
        return new PublisherError<>(error, whenRequested);
    }

    public static <T> PublisherBase<T> error(Supplier<? extends Throwable> errorSupplier) {
        return new PublisherError<>(errorSupplier, false);
    }

    public static <T> PublisherBase<T> error(Supplier<? extends Throwable> errorSupplier, boolean whenRequested) {
        return new PublisherError<>(errorSupplier, whenRequested);
    }

    public static PublisherBase<Integer> range(int start, int count) {
        if (count == 0) {
            return empty();
        }
        if (count == 1) {
            return just(start);
        }
        return new PublisherRange(start, count);
    }
    
    @SafeVarargs
    public static <T> PublisherBase<T> fromArray(T... array) {
        int n = array.length;
        if (n == 0) {
            return empty();
        } else
        if (n == 1) {
            return just(array[0]);
        }
        return new PublisherArray<>(array);
    }

    public static <T> PublisherBase<T> fromIterable(Iterable<? extends T> iterable) {
        return new PublisherIterable<>(iterable);
    }

    public static <T> PublisherBase<T> fromCallable(Callable<? extends T> callable) {
        return new PublisherCallable<>(callable);
    }
    
    @SafeVarargs
    public static <T, R> PublisherBase<R> zip(Function<? super Object[], ? extends R> zipper, Publisher<? extends T>... sources) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }
    
    public static <T, R> PublisherBase<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper) {
        return zipArray(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> PublisherBase<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper) {
        return zipIterable(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> PublisherBase<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch);
    }

    public static <T, R> PublisherBase<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(prefetch), prefetch);
    }
    
    @SafeVarargs
    public static <T> PublisherBase<T> concatArray(Publisher<? extends T>... sources) {
        return new PublisherConcatArray<>(sources);
    }

    public static <T> PublisherBase<T> concatIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return new PublisherConcatIterable<>(sources);
    }

    @SuppressWarnings("unchecked")
    @SafeVarargs
    public static <T> PublisherBase<T> mergeArray(Publisher<? extends T>... sources) {
        return fromArray(sources).flatMap(IDENTITY_FUNCTION);
    }

    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> mergeIterable(Iterable<? extends Publisher<? extends T>> sources) {
        return fromIterable(sources).flatMap(IDENTITY_FUNCTION);
    }

    public static PublisherBase<Long> timer(long delay, TimeUnit unit, ScheduledExecutorService executor) {
        return new PublisherTimer(delay, unit, executor);
    }

    public static PublisherBase<Long> interval(long period, TimeUnit unit, ScheduledExecutorService executor) {
        return interval(period, period, unit, executor);
    }

    public static PublisherBase<Long> interval(long initialDelay,long period, TimeUnit unit, ScheduledExecutorService executor) {
        return new PublisherInterval(initialDelay, period, unit, executor);
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> PublisherBase<R> combineLatest(Publisher<? extends T> p1, Publisher<? extends U> p2, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return new PublisherCombineLatest<>(new Publisher[] { p1, p2 }, a -> combiner.apply((T)a[0], (U)a[1]), defaultUnboundedQueueSupplier(BUFFER_SIZE), BUFFER_SIZE);
    }
    
    @SuppressWarnings("rawtypes")
    static final Function IDENTITY_FUNCTION = new Function() {
        @Override
        public Object apply(Object t) {
            return t;
        }
    };
    
    static Callable<Consumer<Runnable>> fromExecutor(ExecutorService executor) {
        Objects.requireNonNull(executor, "executor");
        return new ExecutorServiceScheduler(executor);
    }
}
