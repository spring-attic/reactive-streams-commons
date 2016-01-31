package reactivestreams.commons.publisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.reactivestreams.Publisher;

import reactivestreams.commons.publisher.internal.PerfAsyncSubscriber;
import reactivestreams.commons.util.ExecutorServiceScheduler;
import reactivestreams.commons.util.SpscArrayQueue;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='RangeFlatMapObserveOnPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class RangeFlatMapObserveOnPerf {
    
    @Param({"1", "1000", "1000000"})
    public int count;
    
    Publisher<Integer> source1;

    Publisher<Integer> source2;

    ExecutorService exec;
    
    @Setup
    public void setup() {
        exec = Executors.newSingleThreadExecutor();
        
        ExecutorServiceScheduler scheduler = new ExecutorServiceScheduler(exec);
        
        PublisherBase<Integer> source = PublisherBase.range(1, count).flatMap(PublisherBase::just);
        
        source1 = source.observeOn(scheduler);
        
        source2 = new PublisherObserveOn<>(source, scheduler, false, 128, () -> new SpscArrayQueue<>(128));
    }
    
    @TearDown
    public void tearDown() {
        exec.shutdown();
    }
    
    @Benchmark
    public void benchDefault(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        
        source1.subscribe(s);
        
        s.await(count);
    }

    @Benchmark
    public void benchSpsc(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        
        source2.subscribe(s);
        
        s.await(count);
    }
}
