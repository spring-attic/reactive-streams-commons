/*
 * Copyright 2015 David Karnok
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package reactivestreams.commons.publisher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import reactivestreams.commons.publisher.internal.PerfAsyncSubscriber;
import reactivestreams.commons.publisher.internal.PerfSubscriber;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ReactiveStreamsImpls {
    @Param({ "1", "1000", "1000000" })
    public int times;
    
    Px<Integer> rscRange;
    Px<Integer> rscRangeFlatMapJust;
    Px<Integer> rscRangeFlatMapRange;
    Px<Integer> rscRangeAsync;
    Px<Integer> rscRangePipeline;

    ScheduledExecutorService exec1;
    ScheduledExecutorService exec2;
    
    @Setup
    public void setup() throws Exception {
        exec1 = Executors.newSingleThreadScheduledExecutor();
        exec2 = Executors.newSingleThreadScheduledExecutor();
        
        rscRange = Px.range(1, times);
        rscRangeFlatMapJust = rscRange.flatMap(Px::just);
        rscRangeFlatMapRange = rscRange.flatMap(v -> Px.range(v, 2));
        rscRangeAsync = rscRange.observeOn(exec1);
        rscRangePipeline = rscRange.subscribeOn(exec1).observeOn(exec2);
    }
    
    @TearDown
    public void teardown() {
        exec1.shutdownNow();
        exec2.shutdownNow();
    }

    @Benchmark
    public void range_rsc(Blackhole bh) {
        rscRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapJust_rsc(Blackhole bh) {
        rscRangeFlatMapJust.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void rangeFlatMapRange_rsc(Blackhole bh) {
        rscRangeFlatMapRange.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void rangeAsync_rsc(Blackhole bh) throws InterruptedException {
        PerfAsyncSubscriber lo = new PerfAsyncSubscriber(bh);
        rscRangeAsync.subscribe(lo);
        lo.await(times);
    }

    @Benchmark
    public void rangePipeline_rsc(Blackhole bh) throws InterruptedException {
        PerfAsyncSubscriber lo = new PerfAsyncSubscriber(bh);
        rscRangePipeline.subscribe(lo);
        lo.await(times);
    }
}