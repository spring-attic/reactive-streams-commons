/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.state.Introspectable;

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
    static <T> Supplier<Queue<T>> defaultQueueSupplier() {
        return (Supplier)QUEUE_SUPPLIER;
    }
    
    public final <R> PublisherBase<R> map(Function<? super T, ? extends R> mapper) {
        return new PublisherMap<>(this, mapper);
    }
    
    public final PublisherBase<T> filter(Predicate<? super T> predicate) {
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
        return new PublisherSwitchMap<>(this, mapper, defaultQueueSupplier(), BUFFER_SIZE);
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
        return new PublisherBufferStartEnd<>(this, start, end, bufferSupplier, defaultQueueSupplier());
    }

    public final <U> PublisherBase<PublisherBase<T>> window(Publisher<U> other) {
        return new PublisherWindowBoundary<>(this, other, defaultQueueSupplier(), defaultQueueSupplier());
    }

    public final <U> PublisherBase<PublisherBase<T>> window(Publisher<U> other, int maxSize) {
        return new PublisherWindowBoundaryAndSize<>(this, other, defaultQueueSupplier(), defaultQueueSupplier(), maxSize);
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
    
    public final PublisherBase<Boolean> contains(T value) {
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
        return new PublisherPeek<>(this, onSubscribe, null, null, null, null, null, null);
    }
    
    public final PublisherBase<T> doOnNext(Consumer<? super T> onNext) {
        return new PublisherPeek<>(this, null, onNext, null, null, null, null, null);
    }

    public final PublisherBase<T> doOnError(Consumer<? super Throwable> onError) {
        return new PublisherPeek<>(this, null, null, onError, null, null, null, null);
    }

    public final PublisherBase<T> doOnComplete(Runnable onComplete) {
        return new PublisherPeek<>(this, null, null, null, onComplete, null, null, null);
    }
    
    public final PublisherBase<T> doAfterTerminate(Runnable onAfterTerminate) {
        return new PublisherPeek<>(this, null, null, null, null, onAfterTerminate, null, null);
    }

    public final PublisherBase<T> doOnRequest(LongConsumer onRequest) {
        return new PublisherPeek<>(this, null, null, null, null, null, onRequest, null);
    }
    
    public final PublisherBase<T> doOnCancel(Runnable onCancel) {
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
        return new PublisherWindowStartEnd<>(this, start, end, defaultQueueSupplier(), defaultQueueSupplier());
    }

    public final <U, V> PublisherBase<PublisherBase<T>> window2(Publisher<U> start, Function<? super U, ? extends Publisher<V>> end) {
        return new PublisherWindowBeginEnd<>(this, start, end, defaultQueueSupplier(), BUFFER_SIZE);
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
        return new PublisherThrottleTimeout<>(this, throttler, defaultQueueSupplier());
    }
    
    public final Iterable<T> toIterable() {
        return toIterable(BUFFER_SIZE);
    }

    public final Iterable<T> toIterable(long batchSize) {
        return new BlockingIterable<>(this, batchSize, defaultQueueSupplier());
    }
    
    public final Stream<T> stream() {
        return stream(BUFFER_SIZE);
    }
    
    public final Stream<T> stream(long batchSize) {
        return new BlockingStream<>(this, batchSize, defaultQueueSupplier()).stream();
    }

    public final Stream<T> parallelStream() {
        return parallelStream(BUFFER_SIZE);
    }
    
    public final Stream<T> parallelStream(long batchSize) {
        return new BlockingStream<>(this, batchSize, defaultQueueSupplier()).parallelStream();
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
        return new PublisherBufferBoundaryAndSize<>(this, other, () -> new ArrayList<>(), maxSize, defaultQueueSupplier());
    }

    public final <U, C extends Collection<? super T>> PublisherBase<C> buffer(Publisher<U> other, int maxSize, Supplier<C> bufferSupplier) {
        return new PublisherBufferBoundaryAndSize<>(this, other, bufferSupplier, maxSize, defaultQueueSupplier());
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
        return new PublisherFlatMap<>(this, mapper, delayError, maxConcurrency, defaultQueueSupplier(), prefetch, defaultQueueSupplier());
    }

    @SuppressWarnings("unchecked")
    public final <U, R> PublisherBase<R> zipWith(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> zipper) {
        return zipArray(new Publisher[] { this, other }, a -> {
            return zipper.apply((T)a[0], (U)a[1]);
        });
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
    
    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> wrap(Publisher<? extends T> source) {
        if (source instanceof PublisherBase) {
            return (PublisherBase<T>)source;
        }
        return new PublisherBaseWrapper<>(source);
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

    @SafeVarargs
    public static <T, R> PublisherBase<R> zip(Function<? super Object[], ? extends R> zipper, Publisher<? extends T>... sources) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(), BUFFER_SIZE);
    }
    
    public static <T, R> PublisherBase<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper) {
        return zipArray(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> PublisherBase<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper) {
        return zipIterable(sources, zipper, BUFFER_SIZE);
    }

    public static <T, R> PublisherBase<R> zipArray(Publisher<? extends T>[] sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(), prefetch);
    }

    public static <T, R> PublisherBase<R> zipIterable(Iterable<? extends Publisher<? extends T>> sources, Function<? super Object[], ? extends R> zipper, int prefetch) {
        return new PublisherZip<>(sources, zipper, defaultQueueSupplier(), prefetch);
    }

}
