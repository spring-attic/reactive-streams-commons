package rsc.parallel;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rsc.publisher.Px;
import rsc.scheduler.*;
import rsc.test.TestSubscriber;

public class ParallelPublisherTest {

    @Test
    public void sequentialMode() {
        Px<Integer> source = Px.range(1, 1_000_000).hide();
        for (int i = 1; i < 33; i++) {
            Px<Integer> result = ParallelPublisher.fork(source, false, i)
            .map(v -> v + 1)
            .join()
            ;
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            result.subscribe(ts);

            ts
            .assertSubscribed()
            .assertValueCount(1_000_000)
            .assertComplete()
            .assertNoError()
            ;
        }
        
    }

    @Test
    public void sequentialModeFused() {
        Px<Integer> source = Px.range(1, 1_000_000);
        for (int i = 1; i < 33; i++) {
            Px<Integer> result = ParallelPublisher.fork(source, false, i)
            .map(v -> v + 1)
            .join()
            ;
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            result.subscribe(ts);

            ts
            .assertSubscribed()
            .assertValueCount(1_000_000)
            .assertComplete()
            .assertNoError()
            ;
        }
        
    }

    @Test
    public void parallelMode() {
        Px<Integer> source = Px.range(1, 1_000_000).hide();
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {
            
            Scheduler scheduler = new ParallelScheduler(i);
            
            try {
                Px<Integer> result = ParallelPublisher.fork(source, false, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .join()
                ;
                
                TestSubscriber<Integer> ts = new TestSubscriber<>();
                
                result.subscribe(ts);
    
                ts.assertTerminated(10, TimeUnit.SECONDS);
                
                ts
                .assertSubscribed()
                .assertValueCount(1_000_000)
                .assertComplete()
                .assertNoError()
                ;
            } finally {
                scheduler.shutdown();
            }
        }
        
    }

    @Test
    public void parallelModeFused() {
        Px<Integer> source = Px.range(1, 1_000_000);
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {
            
            Scheduler scheduler = new ParallelScheduler(i);
            
            try {
                Px<Integer> result = ParallelPublisher.fork(source, false, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .join()
                ;
                
                TestSubscriber<Integer> ts = new TestSubscriber<>();
                
                result.subscribe(ts);
    
                ts.assertTerminated(10, TimeUnit.SECONDS);
                
                ts
                .assertSubscribed()
                .assertValueCount(1_000_000)
                .assertComplete()
                .assertNoError()
                ;
            } finally {
                scheduler.shutdown();
            }
        }
        
    }

}
