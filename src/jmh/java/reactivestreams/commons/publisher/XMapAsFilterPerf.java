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

import reactivestreams.commons.publisher.internal.PerfSubscriber;

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
public class XMapAsFilterPerf {

    @Param({"1", "1000", "1000000"})
    public int count;
    
    @Param({"0", "1", "3", "7", "65535"})
    public int mask;
    
    public Px<Integer> justEmptyFlatMap;
    
    public Px<Integer> rangeEmptyFlatMap;

    public Px<Integer> justEmptyConcatMap;
    
    public Px<Integer> rangeEmptyConcatMap;

    @Setup
    public void setup() {
        if (count == 1 && mask != 0) {
            throw new RuntimeException("Force skip");
        }
        Integer[] values = new Integer[count];
        for (int i = 0; i < count; i++) {
            values[i] = i;
        }
        final Px<Integer> just = Px.just(1);
        
        final Px<Integer> range = Px.range(1, 2);
        
        final Px<Integer> empty = Px.empty();
        
        final int m = mask;
        
        justEmptyFlatMap = Px.fromArray(values).flatMap(v -> (v & m) == 0 ? empty : just);
        
        rangeEmptyFlatMap = Px.fromArray(values).flatMap(v -> (v & m) == 0 ? empty : range);

        justEmptyConcatMap = Px.fromArray(values).concatMap(v -> (v & m) == 0 ? empty : just);
        
        rangeEmptyConcatMap = Px.fromArray(values).concatMap(v -> (v & m) == 0 ? empty : range);
    }

    @Benchmark
    public void justEmptyFlatMap(Blackhole bh) {
        justEmptyFlatMap.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void rangeEmptyFlatMap(Blackhole bh) {
        rangeEmptyFlatMap.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void justEmptyConcatMap(Blackhole bh) {
        justEmptyConcatMap.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void rangeEmptyConcatMap(Blackhole bh) {
        rangeEmptyConcatMap.subscribe(new PerfSubscriber(bh));
    }
}