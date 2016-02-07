package reactivestreams.commons.publisher;

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

import reactivestreams.commons.publisher.internal.PerfSlowPathSubscriber;
import reactivestreams.commons.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherAggregatePerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherAggregatePerf {
    @Param({"1", "1000", "1000000"})
    public int count;
    
    Publisher<Integer> range;
    Publisher<Integer> array;
    
    @Setup
    public void setup() {
        range = PublisherBase.range(0, count).reduce(Integer::max);
        Integer[] arr = new Integer[count];
        for (int i = 0; i < count; i++) {
            arr[i] = i;
        }
        
        array = PublisherBase.fromArray(arr).reduce(Integer::max);
    }
    
    @Benchmark
    public void range(Blackhole bh) {
        range.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void array(Blackhole bh) {
        range.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeSlowpath(Blackhole bh) {
        range.subscribe(new PerfSlowPathSubscriber(bh, count));
    }

    @Benchmark
    public void arraySlowpath(Blackhole bh) {
        range.subscribe(new PerfSlowPathSubscriber(bh, count));
    }
}
