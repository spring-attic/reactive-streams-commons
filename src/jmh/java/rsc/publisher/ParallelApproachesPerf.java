package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import rsc.scheduler.ParallelScheduler;
import rsc.scheduler.Scheduler;
import rsc.util.PerfAsyncSubscriber;

/**
 * Benchmark parallelization approaches.
 * <p>
 * gradle jmh -Pjmh='ParallelApproachesPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParallelApproachesPerf {

    @Param({"10000"})
    public int count;
    
    @Param({"1", "10", "100", "10000"})
    public int compute;
    
    @Param({"1", "2", "3", "4"})
    public int parallelism;
    
    Px<Integer> groupBy;
    
    Px<Integer> window;
    
    Px<Integer> flatMap;

    Px<Integer> flatMapLimit;

    Scheduler scheduler;
    
    @Setup
    public void setup(Blackhole bh) {
        
        scheduler = new ParallelScheduler(parallelism);
        
        Integer[] values = new Integer[count];
        Arrays.fill(values, 0);
        
        Px<Integer> source = Px.fromArray(values);
        
        int[] index = { 0 };
        Function<Integer, Integer> gf = v -> {
            int g = index[0]++;
            if (g + 1 == parallelism) {
                index[0] = 0;
            }
            return g;
        };
        
        Function<Integer, Integer> work = v -> { Blackhole.consumeCPU(compute); return v; };
        
        groupBy = source.groupBy(gf).flatMap(g -> g.observeOn(scheduler).map(work), false, Px.BUFFER_SIZE);
        
        window = source.window(Px.BUFFER_SIZE).flatMap(w -> w.subscribeOn(scheduler).map(work), false, Px.BUFFER_SIZE);
        
        flatMap = source.flatMap(v -> Px.just(v).observeOn(scheduler).map(work), false, Px.BUFFER_SIZE);

        flatMapLimit = source.flatMap(v -> Px.just(v).observeOn(scheduler).map(work), false, parallelism);
    }

    @Benchmark
    public void groupBy(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        groupBy.subscribe(s);
        s.await(count);
    }

    @Benchmark
    public void window(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        window.subscribe(s);
        s.await(count);
    }
    
    @Benchmark
    public void flatMap(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        flatMap.subscribe(s);
        s.await(count);
    }

    @Benchmark
    public void flatMapLimit(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        flatMapLimit.subscribe(s);
        s.await(count);
    }


}