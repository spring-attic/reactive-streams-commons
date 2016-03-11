package reactivestreams.commons.publisher;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='HotMapScanPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class HotMapScanPerf {
    @Benchmark
    public void bench(Blackhole bh) {
        SimpleProcessor<Integer> processor = new SimpleProcessor<>();
        Publisher<Integer> source = processor.map(v -> v + 1).scan(0, (a, b) -> a + b);
        
        source.subscribe(new PerfSubscriber(bh));
        for (int i = 0; i < 1_000_000; i++) {
            processor.onNext(i);
        }
        processor.onComplete();
    }
}
