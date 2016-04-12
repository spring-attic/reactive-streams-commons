package rsc.util;

import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.publisher.*;
import rsc.publisher.internal.PerfAsyncSubscriber;
import rsc.scheduler.ExecutorServiceScheduler;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='QueuePerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class QueuePerf {
    @Param({ "1", "1000", "1000000" })
    public int count;
    
    @Param({ "16", "256", "1024", "65536"})
    public int prefetch;

    ExecutorService exec1;
    ExecutorService exec2;

    Px<Integer> rsc;
    Px<Integer> rscLinked;
    
    @Setup
    public void setup() {
        exec1 = Executors.newSingleThreadExecutor();
        exec2 = Executors.newSingleThreadExecutor();
        
        Integer[] arr = new Integer[count];
        for (int i = 0; i < count; i++) {
            arr[i] = 777;
        }
        
        Px<Integer> source = Px.fromArray(arr).subscribeOn(exec1);
        
        ExecutorServiceScheduler s2 = new ExecutorServiceScheduler(exec2);
        
        rsc = new PublisherObserveOn<>(source, s2, false, prefetch, () -> new SpscArrayQueue<>(prefetch));
        
        rscLinked = new PublisherObserveOn<>(source, s2, false, prefetch, () -> new SpscLinkedArrayQueue<>(prefetch));
    }
    
    @TearDown
    public void teardown() {
        exec1.shutdown();
        exec2.shutdown();
    }
    
    void run(Publisher<Integer> p, Blackhole bh) {
        PerfAsyncSubscriber lo = new PerfAsyncSubscriber(bh);
        
        p.subscribe(lo);
        
        lo.await(count);
    }
    
    @Benchmark
    public void rsc(Blackhole bh) {
        run(rsc, bh);
    }

//    @Benchmark
    public void rscLinked(Blackhole bh) {
        run(rscLinked, bh);
    }
}