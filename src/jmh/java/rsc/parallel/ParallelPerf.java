/**
 * Copyright 2016 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rsc.parallel;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import rsc.publisher.Px;
import rsc.scheduler.*;
import rsc.util.PerfSubscriber;

/**
 * Benchmark ParallelPublisher.
 * <p>
 * gradle jmh -Pjmh='ParallelPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParallelPerf {

    @Param({"10000"})
    public int count;
    
    @Param({"1", "10", "100", "10000"})
    public int compute;
    
    @Param({"1", "2", "3", "4"})
    public int parallelism;
    
    Scheduler scheduler;
    
    Px<Integer> parallel;

    Px<Integer> sequential;

    @Setup
    public void setup(Blackhole bh) {
        
        scheduler = new ParallelScheduler(parallelism);
        
        Integer[] values = new Integer[count];
        Arrays.fill(values, 0);
        
        Px<Integer> source = Px.fromArray(values);
        
        this.parallel = ParallelPublisher.fork(source, false, parallelism)
                .runOn(scheduler)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();
        
        this.sequential = ParallelPublisher.fork(source, false, parallelism)
                .map(v -> {
                    Blackhole.consumeCPU(compute);
                    return v;
                })
                .join();
    }
    
    @TearDown
    public void shutdown() {
        scheduler.shutdown();
    }

    @Benchmark
    public void parallel(Blackhole bh) {
        parallel.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void sequential(Blackhole bh) {
        sequential.subscribe(new PerfSubscriber(bh));
    }

}