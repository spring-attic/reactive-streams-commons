package rsc.publisher;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.*;

import rsc.test.TestSubscriber;
import rsc.util.*;
import rsc.scheduler.SingleTimedScheduler;

public class PublisherIntervalTest {

    SingleTimedScheduler exec;
    
    @Before
    public void before() {
        exec = new SingleTimedScheduler();
    }
    
    @After
    public void after() {
        exec.shutdown();
    }

    @Test
    public void constructors() {
        
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherInterval.class);
        
        ctb.addLong("initialDelay", Long.MIN_VALUE, Long.MAX_VALUE);
        ctb.addLong("period", 0, Long.MAX_VALUE);
        ctb.addRef("unit", TimeUnit.SECONDS);
        ctb.addRef("timedScheduler", exec);

        ctb.test();
    }
    
    @Test
    public void normal() {
        try {
            TestSubscriber<Long> ts = new TestSubscriber<>();
            
            ts.values().add(System.currentTimeMillis());
            
            new PublisherInterval(100, 100, TimeUnit.MILLISECONDS, exec)
            .take(5)
            .map(v -> System.currentTimeMillis()).subscribe(ts);
            
            ts.await(5, TimeUnit.SECONDS);
            
            ts.assertValueCount(6)
            .assertNoError()
            .assertComplete();
            
            List<Long> list = ts.values();
            for (int i = 0; i < list.size() - 1; i++) {
                long diff = list.get(i + 1) - list.get(i);
                
                if (diff < 50 || diff > 150) {
                    Assert.fail("Period failure: " + diff);
                }
            }
            
        } finally {
            exec.shutdown();
        }
    }
    
    @Test
    public void flatMap() throws Exception {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        Px.interval(30, TimeUnit.MILLISECONDS, exec).flatMap(v ->
                Px.fromIterable(Arrays.asList("A"))
                .flatMap(w -> Px.fromCallable(() -> Arrays.asList(1, 2)).subscribeOn(exec).flatMap(Px::fromIterable)))
          .subscribe(ts);
        
        Thread.sleep(5000);
        
        ts.cancel();
        
        ts.assertNoError();
        ts.assertNotComplete();
    }
}
