package rsc.parallel;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import rsc.publisher.Px;
import rsc.scheduler.*;
import rsc.util.PerfAsyncSubscriber;

/**
 * Benchmark ParallelPublisher.
 * <p>
 * gradle jmh -Pjmh='ParallelForkPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParallelForkPerf {

    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
    public int count;
    
    @Param({"1", "2", "3", "4", "5", "6", "7", "8"})
    public int parallelism;

    Scheduler scheduler;
    
    ParallelPublisher<Integer> sync;
    
    ParallelPublisher<Integer> async;
    
    PerfAsyncSubscriber[] subs; 
    
    @Setup
    public void setup(Blackhole bh) {
        Integer[] array = new Integer[count];
        Arrays.fill(array, 777);
        
        sync = Px.fromArray(array).parallel(parallelism);
        
        scheduler = new ParallelScheduler(parallelism);
        
        async = sync.runOn(scheduler);
    }

    void createSubs(Blackhole bh) {
        subs = new PerfAsyncSubscriber[parallelism];
        for (int i = 0; i < parallelism; i++) {
            subs[i] = new PerfAsyncSubscriber(bh);
        }
    }
    
    @TearDown
    public void shutdown() {
        scheduler.shutdown();
    }
    
    @Benchmark
    public void sync(Blackhole bh) {
        createSubs(bh);
        sync.subscribe(subs);
    }
    
    @Benchmark 
    public void async(Blackhole bh) {
        createSubs(bh);
        async.subscribe(subs);
        
        for (PerfAsyncSubscriber ps : subs) {
            ps.await(count);
        }
    }

    @Benchmark
    public void syncReseq(Blackhole bh) {
        PerfAsyncSubscriber sub = new PerfAsyncSubscriber(bh);

        sync.sequential().subscribe(sub);
    }
    
    @Benchmark 
    public void asyncReseq(Blackhole bh) {
        PerfAsyncSubscriber sub = new PerfAsyncSubscriber(bh);
        async.sequential().subscribe(sub);
        
        sub.await(count);
    }

}