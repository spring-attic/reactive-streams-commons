package rsc.publisher;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import rsc.util.PerfSubscriber;

/**
 * Benchmark flatMap/concatMap running over a mixture of normal and empty Observables.
 * <p>
 * gradle jmh -Pjmh='XMapAsFilterPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class PublisherFlattenIterablePerf {

    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
    public int count;
    
    Px<Integer> justPlain;
    
    Px<Integer> justIterable;

    Px<Integer> rangePlain;
    
    Px<Integer> rangeIterable;

    Px<Integer> xrangePlain;
    
    Px<Integer> xrangeIterable;

    Px<Integer> chainPlain;
    
    Px<Integer> chainIterable;

    @Setup
    public void setup() {
        Integer[] values = new Integer[count];
        for (int i = 0; i < count; i++) {
            values[i] = i;
        }
        
        int c = 1_000_000 / count;
        Integer[] xvalues = new Integer[c];
        for (int i = 0; i < c; i++) {
            xvalues[i] = i;
        }
        
        Px<Integer> source = Px.fromArray(values);

        justPlain = source.concatMap(Px::just);
        justIterable = source.concatMapIterable(v -> Collections.singleton(v));

        Px<Integer> range = Px.range(1, 2);
        List<Integer> xrange = Arrays.asList(1, 2);
        
        rangePlain = source.concatMap(v -> range);
        rangeIterable = source.concatMapIterable(v -> xrange);
        
        Px<Integer> xsource = Px.fromArray(xvalues);
        List<Integer> xvaluesList = Arrays.asList(xvalues);
        
        xrangePlain = source.concatMap(v -> xsource);
        xrangeIterable = source.concatMapIterable(v -> xvaluesList);

        chainPlain = xrangePlain.concatMap(Px::just);
        chainIterable = xrangeIterable.concatMapIterable(v -> Collections.singleton(v));
    }
    
//    @Benchmark
    public void justPlain(Blackhole bh) {
        justPlain.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void justIterable(Blackhole bh) {
        justIterable.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void rangePlain(Blackhole bh) {
        rangePlain.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void rangeIterable(Blackhole bh) {
        rangeIterable.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void xrangePlain(Blackhole bh) {
        xrangePlain.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void xrangeIterable(Blackhole bh) {
        xrangeIterable.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void chainPlain(Blackhole bh) {
        chainPlain.subscribe(new PerfSubscriber(bh));
    }

//    @Benchmark
    public void chainIterable(Blackhole bh) {
        chainIterable.subscribe(new PerfSubscriber(bh));
    }

}