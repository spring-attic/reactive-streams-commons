/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.publisher.internal.PerfAsyncSubscriber;
import reactivestreams.commons.util.ExecutorServiceScheduler;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='CrossRangeFlatMapObserveOnPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
@Timeout(time = 10, timeUnit = TimeUnit.SECONDS)
public class CrossRangeFlatMapObserveOnPerf {
    
    @Param({"1", "1000", "1000000"})
    public int count;
    
    @Param({"1", "2", "32", "128", "-1"})
    public int maxConcurrency;
    
    Publisher<Integer> source1;

    ExecutorService exec;
    
    @Setup
    public void setup() {
        exec = Executors.newSingleThreadExecutor();
        
        ExecutorServiceScheduler scheduler = new ExecutorServiceScheduler(exec);

        int m = maxConcurrency < 0 ? Integer.MAX_VALUE : maxConcurrency;
        
        source1 = Px.range(1, count).flatMap(v -> Px.range(v, 2), false, m).observeOn(scheduler);
    }
    
    @TearDown
    public void tearDown() {
        exec.shutdown();
    }

    @Benchmark
    public void bench(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);

        source1.subscribe(s);

        s.await(count);
    }
}
