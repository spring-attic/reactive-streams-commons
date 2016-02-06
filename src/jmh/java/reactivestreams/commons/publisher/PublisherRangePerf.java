package reactivestreams.commons.publisher;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.publisher.internal.*;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherRangePerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherRangePerf {

    @Param({"1", "1000", "1000000"})
    int count;

    Publisher<Integer> source;

    PerfSubscriber sharedSubscriber;

    @Setup
    public void setup(Blackhole bh) {
        source = new PublisherRange(0, count);
        sharedSubscriber = new PerfSubscriber(bh);
    }

    @Benchmark
    public void standard(Blackhole bh) {
        source.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void shared() {
        source.subscribe(sharedSubscriber);
    }

    @Benchmark
    public void createNew(Blackhole bh) {
        Publisher<Integer> p = new PublisherRange(0, count);
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void standardSlowpath(Blackhole bh) {
        source.subscribe(new PerfSlowPathSubscriber(bh, count + 1));
    }
}
