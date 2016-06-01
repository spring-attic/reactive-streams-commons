package rsc.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import rsc.publisher.Px;
import rsc.scheduler.ExecutorServiceScheduler;
import rsc.scheduler.ForkJoinScheduler;
import rsc.scheduler.ParallelScheduler;
import rsc.scheduler.Scheduler;
import rsc.util.PerfAsyncSubscriber;
import rsc.util.PerfSubscriber;

/**
 * Benchmark ParallelPublisher.
 * <p>
 * gradle jmh -Pjmh='ParallelPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParallelPerf {

    @Param({"10000"})
    public int count;
    
    @Param({"1", "10", "100", "1000", "10000"})
    public int compute;
    
    @Param({"1", "2", "3", "4"})
    public int parallelism;
    
    Scheduler scheduler;
    
    Scheduler fjScheduler;
    
    Scheduler executor;
    
    ExecutorService exec;
    
    Px<Integer> parallel;

    Px<Integer> parallelFj;

    Px<Integer> parallelExec;

    Px<Integer> parallelCp;

    Px<Integer> sequential;

    @Setup
    public void setup() {
        
        exec = Executors.newFixedThreadPool(parallelism);
        
        scheduler = new ParallelScheduler(parallelism);
        
        fjScheduler = new ForkJoinScheduler(parallelism);
        
        executor = new ExecutorServiceScheduler(exec, false);
        
        Scheduler cpScheduler = new ExecutorServiceScheduler(ForkJoinPool.commonPool(), false);
        
        Integer[] values = new Integer[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        
        Px<Integer> source = Px.fromArray(values);
        
        this.parallel = ParallelPublisher.fork(source, false, parallelism)
                .runOn(scheduler)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();

        this.parallelFj = ParallelPublisher.fork(source, false, parallelism)
                .runOn(fjScheduler)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();

        this.parallelExec = ParallelPublisher.fork(source, false, parallelism)
                .runOn(executor)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();

        this.parallelCp = ParallelPublisher.fork(source, false, parallelism)
                .runOn(cpScheduler)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();

        
        this.sequential = ParallelPublisher.fork(source, false, parallelism)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();
    }
    
    @TearDown
    public void shutdown() {
        scheduler.shutdown();
        
        fjScheduler.shutdown();
        
        executor.shutdown();
        
        exec.shutdown();
    }

    @Benchmark
    public void parallel(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        parallel.subscribe(s);
        s.await(10000);
    }

//    @Benchmark
    public void sequential(Blackhole bh) {
        sequential.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void parallelFj(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        parallelFj.subscribe(s);
        s.await(10000);
    }

    @Benchmark
    public void parallelExec(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        parallelExec.subscribe(s);
        s.await(10000);
    }

    @Benchmark
    public void parallelCp(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        parallelCp.subscribe(s);
        s.await(10000);
    }
}