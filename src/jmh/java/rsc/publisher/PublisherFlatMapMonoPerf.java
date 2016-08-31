package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Processor;

import rsc.processor.DirectProcessor;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherFlatMapMonoPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherFlatMapMonoPerf {
    @Param({"1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024"})
    public int maxConcurrency;
    
    Px<Integer> source;
    
    Processor<Integer, Integer> p;
    
    @Setup
    public void setup() {
        Integer[] array = new Integer[1_000_000];
        Arrays.fill(array, 0);
        
        source = Px.fromArray(array).flatMap(v -> p, false, maxConcurrency);
    }
    
    @Benchmark
    public Object complete() {
        p = new DirectProcessor<>();
        
        Object o = source.subscribe();
        
        p.onComplete();
        
        return o;
    }
}
