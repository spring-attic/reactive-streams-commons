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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import rsc.util.BackpressureHelper;

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
public class BackpressureHelperPerf {

    final AtomicInteger value = new AtomicInteger();

    volatile int wip;

    static final AtomicIntegerFieldUpdater<BackpressureHelperPerf> WIP =
      AtomicIntegerFieldUpdater.newUpdater(BackpressureHelperPerf.class, "wip");

    final AtomicLong req = new AtomicLong();

    volatile long requested;

    static final AtomicLongFieldUpdater<BackpressureHelperPerf> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(BackpressureHelperPerf.class, "requested");

    final AtomicLong reqMax = new AtomicLong();

    volatile long requestedMax;

    static final AtomicLongFieldUpdater<BackpressureHelperPerf> REQUESTED_MAX =
      AtomicLongFieldUpdater.newUpdater(BackpressureHelperPerf.class, "requestedMax");

    @Setup
    public void setup() {
        reqMax.set(Long.MAX_VALUE);
        requestedMax = Long.MAX_VALUE;
    }

    @Benchmark
    public void atomicDecrementAndGet() {
        value.decrementAndGet();
    }

    @Benchmark
    public void atomicCompareAndSet() {
        value.compareAndSet(0, 0);
    }

    @Benchmark
    public void atomicGet() {
        value.get();
    }

    @Benchmark
    public void atomicGetAndSet() {
        value.getAndSet(0);
    }

    @Benchmark
    public void atomicFieldDecrementAndGet() {
        WIP.decrementAndGet(this);
    }

    @Benchmark
    public void atomicFieldCompareAndSet() {
        WIP.compareAndSet(this, 0, 0);
    }

    @Benchmark
    public void atomicFieldGet() {
        WIP.get(this);
    }

    @Benchmark
    public void atomicFieldGetAndSet() {
        WIP.getAndSet(this, 0);
    }

    @Benchmark
    public void requestField() {
        BackpressureHelper.getAndAddCap(REQUESTED, this, 1);
    }

    @Benchmark
    public void requestFieldMax() {
        BackpressureHelper.getAndAddCap(REQUESTED_MAX, this, 1);
    }
}
