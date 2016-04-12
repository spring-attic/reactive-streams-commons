package rsc.publisher;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.util.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherMapPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherMapPerf {
    
    Publisher<Integer> simple;

    Publisher<Integer> rangeFlatMapRange;
    
    Publisher<Integer> rangeConcatMapRange;
    
    @Setup
    public void setup() {
        simple = Px.range(1, 1_000_000).map(v -> v + 1);
        
        rangeFlatMapRange = Px.range(1, 1000).flatMap(v -> Px.range(1, 1000).map(w -> w + 1));

        rangeConcatMapRange = Px.range(1, 1000).concatMap(v -> Px.range(1, 1000).map(w -> w + 1));
    }
    
    @Benchmark
    public void simple(Blackhole bh) {
        simple.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapRange(Blackhole bh) {
        rangeFlatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeConcatMapRange(Blackhole bh) {
        rangeConcatMapRange.subscribe(new PerfSubscriber(bh));
    }

}
