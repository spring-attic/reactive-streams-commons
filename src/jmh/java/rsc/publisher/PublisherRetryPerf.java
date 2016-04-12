package rsc.publisher;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.publisher.internal.PerfSubscriber;

import java.util.concurrent.TimeUnit;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherRetryPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherRetryPerf {

    @Param({"1", "1000", "1000000"})
    int count;

    Integer[] array;

    Publisher<Integer> source;

    PerfSubscriber sharedSubscriber;

    @Setup
    public void setup(Blackhole bh) {
        array = new Integer[count];
        for (int i = 0; i < count; i++) {
            array[i] = 777;
        }
        source = createSource();
        sharedSubscriber = new PerfSubscriber(bh);
    }

    Publisher<Integer> createSource() {
        return new PublisherRetry<>(new PublisherArray<>(array));
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
        Publisher<Integer> p = createSource();
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }
}
