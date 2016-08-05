package rsc.scheduler;

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

import rsc.publisher.Px;
import rsc.util.PerfAsyncSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='SingleSchedulerPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class SingleSchedulerPerf {

    @Param({"1", "1000", "1000000"})
    int count;

    Publisher<Integer> source;

    @Setup
    public void setup(Blackhole bh) {
        Integer[] array = new Integer[count];
        for (int i = 0; i < count; i++) {
            array[i] = i;
        }
        
        SingleScheduler s1 = new SingleScheduler();
        SingleScheduler s2 = new SingleScheduler();
        
        source = Px.fromArray(array).subscribeOn(s1).observeOn(s2);
    }

    @Benchmark
    public void pipeline(Blackhole bh) {
        PerfAsyncSubscriber ps = new PerfAsyncSubscriber(bh);
        source.subscribe(ps);
        ps.await(count);
    }
}
