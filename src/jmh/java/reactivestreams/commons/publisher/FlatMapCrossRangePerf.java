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

package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import reactivestreams.commons.publisher.internal.*;

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
public class FlatMapCrossRangePerf {

    @Param({"10", "100", "1000", "10000", "100000", "1000000"})
    public int count;
    
    public static final int TOTAL = 1_000_000;
    
    public Px<Integer> source;

    public Px<Integer> sourceFused;

    public Px<Integer> asyncSource;

    public Px<Integer> asyncSourceFused;

    public ExecutorService exec;
    
    @Setup
    public void setup() {
        exec = Executors.newSingleThreadExecutor();

        int m = TOTAL / count;
        
        Integer[] first = new Integer[m];
        Arrays.fill(first, 777);
        
        Integer[] second = new Integer[count];
        
        Arrays.fill(second, 888);
        
        Px<Integer> secondSourceFused = Px.fromArray(second);
        Px<Integer> secondSource = secondSourceFused.hide();
        
        source = Px.fromArray(first).flatMap(v -> secondSource);

        sourceFused = Px.fromArray(first).flatMap(v -> secondSourceFused);
        
        asyncSource = source.observeOn(exec);
        
        asyncSourceFused = sourceFused.observeOn(exec);
    }
    
    @TearDown
    public void teardown() {
        exec.shutdownNow();
    }

    @Benchmark
    public void syncSource(Blackhole bh) {
        source.subscribe(new PerfSubscriber(bh));
    }
    @Benchmark
    public void asyncSource(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        asyncSource.subscribe(s);
        s.await(TOTAL);
    }

    @Benchmark
    public void syncSourceFused(Blackhole bh) {
        sourceFused.subscribe(new PerfSubscriber(bh));
    }
    @Benchmark
    public void asyncSourceFused(Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        asyncSourceFused.subscribe(s);
        s.await(TOTAL);
    }

}