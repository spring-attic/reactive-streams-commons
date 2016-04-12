package rsc.publisher;

import java.util.ArrayDeque;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import rsc.processor.UnicastProcessor;
import rsc.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='UnicastProcessorPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class UnicastProcessorPerf {

    @Param({"1", "1000", "1000000"})
    int count;

    @Benchmark
    public void loadCLQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        bh.consume(q);
    }

    @Benchmark
    public void loadADQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ArrayDeque<>());
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        bh.consume(q);
    }

    @Benchmark
    public void replayCLQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        q.subscribe(new PerfSubscriber(bh));
        bh.consume(q);
    }

    @Benchmark
    public void replayADQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ArrayDeque<>());
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        q.subscribe(new PerfSubscriber(bh));
        bh.consume(q);
    }

    @Benchmark
    public void passthroughCLQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        q.subscribe(new PerfSubscriber(bh));
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        bh.consume(q);
    }

    @Benchmark
    public void passthroughADQ(Blackhole bh) {
        int s = count;
        UnicastProcessor<Integer> q = new UnicastProcessor<>(new ArrayDeque<>());
        q.subscribe(new PerfSubscriber(bh));
        for (int i = 0; i < s; i++) {
            q.onNext(777);
        }
        bh.consume(q);
    }

}
