package reactivestreams.commons;

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

import reactivestreams.commons.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherConcatIterablePerf'
 * 
 *
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherConcatIterablePerf {

    @Param({"1", "1000", "1000000"})
    int count;
    
    Publisher<Integer> source1;
    Publisher<Integer> source2;
    Publisher<Integer> source3;
    
    PerfSubscriber sharedSubscriber;
    
    @Setup
    public void setup(Blackhole bh) {
        source1 = createSource1();
        source2 = createSource2();
        source3 = createSource3();
        sharedSubscriber = new PerfSubscriber(bh);
    }
    
    Publisher<Integer> createSource1() {
        return new PublisherConcatIterable<>(Arrays.asList(new PublisherRange(0, count), PublisherEmpty.<Integer>instance()));
    }

    Publisher<Integer> createSource2() {
        return new PublisherConcatIterable<>(Arrays.asList(PublisherEmpty.<Integer>instance(), new PublisherRange(0, count)));
    }

    Publisher<Integer> createSource3() {
        return new PublisherConcatIterable<>(Arrays.asList(new PublisherRange(0, count / 2), new PublisherRange(0, count / 2)));
    }

    @Benchmark
    public void standard1(Blackhole bh) {
        source1.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void shared1() {
        source1.subscribe(sharedSubscriber);
    }
    
    @Benchmark
    public void createNew1(Blackhole bh) {
        Publisher<Integer> p = createSource1();
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void standard2(Blackhole bh) {
        source2.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void shared2() {
        source2.subscribe(sharedSubscriber);
    }
    
    @Benchmark
    public void createNew2(Blackhole bh) {
        Publisher<Integer> p = createSource2();
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void standard3(Blackhole bh) {
        source3.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void shared3() {
        source3.subscribe(sharedSubscriber);
    }
    
    @Benchmark
    public void createNew3(Blackhole bh) {
        Publisher<Integer> p = createSource3();
        bh.consume(p);
        p.subscribe(new PerfSubscriber(bh));
    }

}
