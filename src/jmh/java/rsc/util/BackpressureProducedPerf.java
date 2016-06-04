package rsc.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='BackpressureHelperPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class BackpressureProducedPerf {

    final AtomicInteger value = new AtomicInteger();

    volatile int wip;

    static final AtomicIntegerFieldUpdater<BackpressureProducedPerf> WIP =
      AtomicIntegerFieldUpdater.newUpdater(BackpressureProducedPerf.class, "wip");

    final AtomicLong req = new AtomicLong();

    volatile long requested;

    static final AtomicLongFieldUpdater<BackpressureProducedPerf> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(BackpressureProducedPerf.class, "requested");

    @Benchmark
    public void wipFieldCas() {
        int w = wip;
        WIP.compareAndSet(this, w, w - 1);
    }

    @Benchmark
    public void wipFieldDec() {
        WIP.addAndGet(this, -1);
    }

    @Benchmark
    public void wipCas() {
        AtomicInteger v = value;
        int w = v.get();
        v.compareAndSet(w, w - 1);
    }

    @Benchmark
    public void wipDec() {
        value.addAndGet(-1);
    }

    @Benchmark
    public void reqFieldCas() {
        long r = requested;
        REQUESTED.compareAndSet(this, r, r - 1);
    }

    @Benchmark
    public void reqFieldDec() {
        REQUESTED.addAndGet(this, -1);
    }

    @Benchmark
    public void reqCas() {
        AtomicLong q = req;
        long r = q.get();
        q.compareAndSet(r, r - 1);
    }

    @Benchmark
    public void reqDec() {
        req.addAndGet(-1);
    }

}
