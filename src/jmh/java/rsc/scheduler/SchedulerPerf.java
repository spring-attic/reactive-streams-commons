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

package rsc.scheduler;

import java.util.EnumMap;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import reactor.core.publisher.Computations;
import rsc.flow.Cancellation;
import rsc.scheduler.Scheduler.Worker;

/**
 * Benchmark flatMap/concatMap running over a mixture of normal and empty Observables.
 * <p>
 * gradle jmh -Pjmh='SchedulerPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class SchedulerPerf {

    @Param({"1", "1000", "1000000"})
    public int count;
    
    @Param
    public SchedulerType type;

    public enum SchedulerType {
        SINGLE(true),
        PARALLEL,
        
        EXECUTOR_SINGLE(true),
        EXECUTOR_SINGLE_TRAMPOLINE,

        EXECUTOR_MANY,
        EXECUTOR_MANY_TRAMPOLINE,

        FORKJOIN,
        FORKJOIN_TRAMPOLINE,

        SCHEDULED_EXECUTOR_SINGLE(true),
        SCHEDULED_EXECUTOR_SINGLE_TRAMPOLINE,

        SCHEDULED_EXECUTOR_MANY,
        SCHEDULED_EXECUTOR_MANY_TRAMPOLINE,

        TIMED(true),
        
        TIMED_SINGLE(true),
        
        TIMED_MANY
        
        ;
        
        final boolean orderedDirect;
        SchedulerType() {
            orderedDirect = false;
        }
        
        SchedulerType(boolean orderedDirect) {
            this.orderedDirect = orderedDirect;
        }
    }
    
    EnumMap<SchedulerType, Scheduler> schedulers;
    
    ExecutorService executorServiceSingle;

    ExecutorService executorServiceMany;

    ScheduledExecutorService scheduledExecutorServiceSingle;

    ScheduledExecutorService scheduledExecutorSingleMany;

    Scheduler single;
    
    Scheduler parallel;

    // wrapping an ExecutorService
    
    Scheduler executorSingle;
    
    Scheduler executorTrampolineSingle;

    Scheduler executorMany;
    
    Scheduler executorTrampolineMany;

    // Wrapping ForkJoinPool.commonPool()
    
    Scheduler forkjoin;

    Scheduler forkjoinTrampoline;

    // wrapping a ScheduledExecutorService instance
    
    Scheduler scheduledExecutorSingle;
    
    Scheduler scheduledExecutorTrampolineSingle;

    Scheduler scheduledExecutorMany;
    
    Scheduler scheduledExecutorTrampolineMany;
    
    
    TimedScheduler timedSingle;

    TimedScheduler timedMany;
    
    TimedScheduler timed;

    @Setup
    public void setup() {
        
        int ncpu = Runtime.getRuntime().availableProcessors();
        
        executorServiceSingle = Executors.newSingleThreadExecutor();
        
        executorServiceMany = Executors.newFixedThreadPool(ncpu);

        scheduledExecutorServiceSingle = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorSingleMany = Executors.newScheduledThreadPool(ncpu);

        // -----------------------------------------------------------------------------------
        
        single = new ParallelScheduler(1);
        
        parallel = new ParallelScheduler();
        
        
        executorSingle = new ExecutorServiceScheduler(executorServiceSingle, false);

        executorTrampolineSingle = new ExecutorServiceScheduler(executorServiceSingle, true);

        executorMany = new ExecutorServiceScheduler(executorServiceMany, false);

        executorTrampolineMany = new ExecutorServiceScheduler(executorServiceMany, true);

        
        
        
        forkjoin = new ExecutorServiceScheduler(ForkJoinPool.commonPool(), false);

        forkjoinTrampoline = new ExecutorServiceScheduler(ForkJoinPool.commonPool(), false);

        
        
        scheduledExecutorSingle = new ExecutorServiceScheduler(scheduledExecutorServiceSingle, false);

        scheduledExecutorTrampolineSingle = new ExecutorServiceScheduler(scheduledExecutorServiceSingle, true);

        scheduledExecutorMany = new ExecutorServiceScheduler(scheduledExecutorSingleMany, false);

        scheduledExecutorTrampolineMany = new ExecutorServiceScheduler(scheduledExecutorSingleMany, true);

        
        
        timed = new SingleTimedScheduler();
        
        timedSingle = new ExecutorTimedScheduler(scheduledExecutorServiceSingle);
        
        timedMany = new ExecutorTimedScheduler(scheduledExecutorSingleMany);
        
        // -------------------
        
        schedulers = new EnumMap<>(SchedulerType.class);
        
        schedulers.put(SchedulerType.SINGLE, single);
        schedulers.put(SchedulerType.PARALLEL, parallel);
        
        schedulers.put(SchedulerType.EXECUTOR_SINGLE, executorSingle);
        schedulers.put(SchedulerType.EXECUTOR_SINGLE_TRAMPOLINE, executorTrampolineSingle);
        schedulers.put(SchedulerType.EXECUTOR_MANY, executorMany);
        schedulers.put(SchedulerType.EXECUTOR_MANY_TRAMPOLINE, executorTrampolineMany);
        
        schedulers.put(SchedulerType.FORKJOIN, forkjoin);
        schedulers.put(SchedulerType.FORKJOIN_TRAMPOLINE, forkjoin);
        
        schedulers.put(SchedulerType.SCHEDULED_EXECUTOR_SINGLE, scheduledExecutorSingle);
        schedulers.put(SchedulerType.SCHEDULED_EXECUTOR_SINGLE_TRAMPOLINE, scheduledExecutorTrampolineSingle);
        schedulers.put(SchedulerType.SCHEDULED_EXECUTOR_MANY, scheduledExecutorMany);
        schedulers.put(SchedulerType.SCHEDULED_EXECUTOR_MANY_TRAMPOLINE, scheduledExecutorTrampolineMany);
        
        schedulers.put(SchedulerType.TIMED, timed);
        schedulers.put(SchedulerType.TIMED_SINGLE, timedSingle);
        schedulers.put(SchedulerType.TIMED_MANY, timedMany);
    }
    
    @TearDown
    public void teardown() {
        executorServiceSingle.shutdownNow();

        executorServiceMany.shutdownNow();

        scheduledExecutorServiceSingle.shutdownNow();

        scheduledExecutorSingleMany.shutdownNow();

        single.shutdown();
        
        parallel.shutdown();
        
        timed.shutdown();
    }
    
    void runUnordered(Blackhole bh, int n, Scheduler scheduler) {
        CountDownLatch cdl = new CountDownLatch(n);
        
        for (int i = 0; i < n; i++) {
            scheduler.schedule(() -> {
                cdl.countDown();
            });
        }
        
        if (n <= 1000) {
            while (cdl.getCount() != 0L) ;
        } else {
            try {
                cdl.await();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    void runOrdered(Blackhole bh, int n, Scheduler scheduler) {
        CountDownLatch cdl = new CountDownLatch(1);
        
        for (int i = 0; i < n; i++) {
            scheduler.schedule(() -> {
                bh.consume(true);
            });
        }
        scheduler.schedule(cdl::countDown);
        
        if (n <= 1000) {
            while (cdl.getCount() != 0L) ;
        } else {
            try {
                cdl.await();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    void runOrderedWorker(Blackhole bh, int n, Scheduler scheduler) {
        
        Worker w = scheduler.createWorker();
        
        try {
            CountDownLatch cdl = new CountDownLatch(1);
            
            for (int i = 0; i < n; i++) {
                scheduler.schedule(() -> {
                    bh.consume(true);
                });
            }
            scheduler.schedule(cdl::countDown);
            
            if (n <= 1000) {
                while (cdl.getCount() != 0L) ;
            } else {
                try {
                    cdl.await();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            w.shutdown();
        }
    }
    
    @Benchmark
    public void direct_unordered(Blackhole bh) {
        runUnordered(bh, count, schedulers.get(type));
    }
    
    @Benchmark
    public void direct_ordered(Blackhole bh) {
        if (type.orderedDirect) {
            runOrdered(bh, count, schedulers.get(type));
        } else {
            runUnordered(bh, count, schedulers.get(type));
        }
    }

    @Benchmark
    public void worker_ordered(Blackhole bh) {
        runOrderedWorker(bh, count, schedulers.get(type));
    }

    static final class ReactorScheduler implements Scheduler {
        static final Cancellation NOOP = () -> {};

        final reactor.core.scheduler.Scheduler scheduler;

        ReactorScheduler(reactor.core.scheduler.Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public Cancellation schedule(Runnable task) {
            scheduler.schedule(task);
            return NOOP;
        }

        @Override
        public Worker createWorker() {
            return new ReactorWorker(scheduler.createWorker());
        }

        static final class ReactorWorker implements Worker {

            final reactor.core.scheduler.Scheduler.Worker w;

            ReactorWorker(reactor.core.scheduler.Scheduler.Worker w) {
                this.w = w;
            }

            @Override
            public Cancellation schedule(Runnable task) {
                w.schedule(task);
                return NOOP;
            }

            @Override
            public void shutdown() {
                w.shutdown();
            }
        }
    }
}