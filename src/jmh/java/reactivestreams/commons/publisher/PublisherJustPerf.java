package reactivestreams.commons.publisher;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.publisher.internal.PerfSubscriber;

import java.util.concurrent.TimeUnit;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherJustPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherJustPerf {

    Publisher<Integer> just;

    PerfSubscriber sharedSubscriber;

    @Setup
    public void setup(Blackhole bh) {
        just = new PublisherJust<>(777);
        sharedSubscriber = new PerfSubscriber(bh);
    }

    @Benchmark
    public void standard(Blackhole bh) {
        just.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void shared() {
        just.subscribe(sharedSubscriber);
    }

    @Benchmark
    public void createNew(Blackhole bh) {
        Publisher<Integer> p = new PublisherJust<>(777);
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }
}
