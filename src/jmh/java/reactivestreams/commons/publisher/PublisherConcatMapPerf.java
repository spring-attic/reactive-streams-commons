package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.publisher.PublisherConcatMap.ErrorMode;
import reactivestreams.commons.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherConcatMapPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherConcatMapPerf {

    @State(Scope.Thread)
    public static class ConcatRegular { 
        @Param({"1", "1000", "1000000"})
        int count;
        
        @Param({"true", "false"})
        boolean end;

        Publisher<Integer> baseline;

        Publisher<Integer> justConcatMapRange;

        Publisher<Integer> rangeConcatMapJust;

        Publisher<Integer> justConcatMapArray;

        @Setup
        public void setup() {
            baseline = PublisherBase.range(1, count);

            justConcatMapRange = PublisherBase.just(1).concatMap(v -> PublisherBase.range(v, count), end ? ErrorMode.END : ErrorMode.IMMEDIATE);

            Integer[] arr = new Integer[count];
            Arrays.fill(arr, 777);
            
            justConcatMapArray = PublisherBase.just(1).concatMap(v -> new PublisherArray<>(arr), end ? ErrorMode.END : ErrorMode.IMMEDIATE);

            rangeConcatMapJust = PublisherBase.range(1, count).concatMap(PublisherBase::just, end ? ErrorMode.END : ErrorMode.IMMEDIATE);
        }
    }

    @State(Scope.Thread)
    public static class ConcatCrossRange { 
        Publisher<Integer> justConcatMapJust;

        Publisher<Integer> rangeConcatMapRange;

        Publisher<Integer> rangeConcatMapArray;

        @Param({"true", "false"})
        boolean end;

        @Setup
        public void setup() {
            justConcatMapJust = PublisherBase.just(1).concatMap(v -> PublisherBase.just(v), end ? ErrorMode.END : ErrorMode.IMMEDIATE);
            
            Integer[] arr = new Integer[1000];
            Arrays.fill(arr, 777);

            rangeConcatMapRange = PublisherBase.range(0, 1000).concatMap(v -> PublisherBase.range(v, 1000), end ? ErrorMode.END : ErrorMode.IMMEDIATE);

            rangeConcatMapArray = PublisherBase.range(0, 1000).concatMap(v -> new PublisherArray<>(arr), end ? ErrorMode.END : ErrorMode.IMMEDIATE);
        }
    }

    @Benchmark
    public void baseline(ConcatRegular o, Blackhole bh) {
        o.baseline.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justConcatMapRange(ConcatRegular o, Blackhole bh) {
        o.justConcatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justConcatMapArray(ConcatRegular o, Blackhole bh) {
        o.justConcatMapArray.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeConcatMapJust(ConcatRegular o, Blackhole bh) {
        o.rangeConcatMapJust.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justConcatMapJust(ConcatCrossRange o, Blackhole bh) {
        o.justConcatMapJust.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeConcatMapRange(ConcatCrossRange o, Blackhole bh) {
        o.rangeConcatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeConcatMapArray(ConcatCrossRange o, Blackhole bh) {
        o.rangeConcatMapArray.subscribe(new PerfSubscriber(bh));
    }

    @State(Scope.Thread)
    public static class MergeRegular { 
        @Param({"1", "1000", "1000000"})
        int count;

        Publisher<Integer> justFlatMapRange;

        Publisher<Integer> rangeFlatMapJust;

        Publisher<Integer> justFlatMapArray;

        @Setup
        public void setup() {
            justFlatMapRange = PublisherBase.just(1).flatMap(v -> PublisherBase.range(v, count), false, 1);

            Integer[] arr = new Integer[count];
            Arrays.fill(arr, 777);
            
            justFlatMapArray = PublisherBase.just(1).flatMap(v -> new PublisherArray<>(arr), false, 1);

            rangeFlatMapJust = PublisherBase.range(1, count).flatMap(PublisherBase::just, false, 1);
        }
    }

    @State(Scope.Thread)
    public static class MergeCrossRange { 
        Publisher<Integer> justFlatMapJust;

        Publisher<Integer> rangeFlatMapRange;

        Publisher<Integer> rangeFlatMapArray;

        @Setup
        public void setup() {
            justFlatMapJust = PublisherBase.just(1).flatMap(v -> PublisherBase.just(v), false, 1);
            
            Integer[] arr = new Integer[1000];
            Arrays.fill(arr, 777);

            rangeFlatMapRange = PublisherBase.range(0, 1000).flatMap(v -> PublisherBase.range(v, 1000), false, 1);

            rangeFlatMapArray = PublisherBase.range(0, 1000).flatMap(v -> new PublisherArray<>(arr), false, 1);
        }
    }

    @Benchmark
    public void justFlatMapRange(MergeRegular o, Blackhole bh) {
        o.justFlatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justFlatMapArray(MergeRegular o, Blackhole bh) {
        o.justFlatMapArray.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapJust(MergeRegular o, Blackhole bh) {
        o.rangeFlatMapJust.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justFlatMapJust(MergeCrossRange o, Blackhole bh) {
        o.justFlatMapJust.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapRange(MergeCrossRange o, Blackhole bh) {
        o.rangeFlatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapArray(MergeCrossRange o, Blackhole bh) {
        o.rangeFlatMapArray.subscribe(new PerfSubscriber(bh));
    }

}
