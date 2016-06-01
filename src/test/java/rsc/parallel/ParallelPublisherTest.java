package rsc.parallel;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rsc.publisher.Px;
import rsc.scheduler.*;
import rsc.test.TestSubscriber;

public class ParallelPublisherTest {

    @Test
    public void sequentialMode() {
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
        Px<Integer> source = Px.range(1, 1_000_000);
        for (int i = 1; i < Runtime.getRuntime().availableProcessors() + 1; i++) {
            
            Scheduler scheduler = new ParallelScheduler(i);
            
            Px<Integer> result = ParallelPublisher.fork(source, false, i)
            .runOn(scheduler)
            .map(v -> v + 1)
            .join()
            ;
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            result.subscribe(ts);

            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts
            .assertSubscribed()
            .assertValueCount(1_000_000)
            .assertComplete()
            .assertNoError()
            ;
        }
        
    }
}
