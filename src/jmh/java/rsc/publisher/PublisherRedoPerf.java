package rsc.publisher;

import java.util.Arrays;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherRedoPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherRedoPerf {

    @Param({"1,1", "1,1000", "1,1000000", "1000,1", "1000,1000", "1000000,1"})
    public String params;
    
    public int len;
    public int repeat;
    
    Publisher<Integer> sourceRepeating;
    
    Publisher<Integer> sourceRetrying;
    
    Publisher<Integer> redoRepeating;
    
    Publisher<Integer> redoRetrying;

    Publisher<Integer> baseline;
    
    @Setup
    public void setup() {
        String[] ps = params.split(",");
        len = Integer.parseInt(ps[0]);
        repeat = Integer.parseInt(ps[1]);        
    
        Integer[] values = new Integer[len];
        Arrays.fill(values, 777);
        
        Px<Integer> source = new PublisherArray<>(values);
        
        Px<Integer> error = source.concatWith(new PublisherError<Integer>(new RuntimeException()));
        
        Integer[] values2 = new Integer[len * repeat];
        Arrays.fill(values2, 777);
        
        baseline = new PublisherArray<>(values2); 
        
        sourceRepeating = source.repeat(repeat);
        
        sourceRetrying = error.retry(repeat);
        
        redoRepeating = source.repeatWhen(v -> v).take(len * repeat);

        redoRetrying = error.retryWhen(v -> v).take(len * repeat);
    }
    
    @Benchmark
    public void baseline(Blackhole bh) {
        baseline.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void repeatCounted(Blackhole bh) {
        sourceRepeating.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void retryCounted(Blackhole bh) {
        sourceRetrying.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void repeatWhen(Blackhole bh) {
        redoRepeating.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void retryWhen(Blackhole bh) {
        redoRetrying.subscribe(new PerfSubscriber(bh));
    }

}
