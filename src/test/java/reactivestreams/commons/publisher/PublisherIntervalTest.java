package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherIntervalTest {

    ScheduledExecutorService exec;
    
    @Before
    public void before() {
        exec = Executors.newScheduledThreadPool(1);
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
        ctb.addRef("executor", exec);
        ctb.addRef("asyncExecutor", (BiFunction<Runnable, Long, Runnable>)(r, a) -> r);
        ctb.addRef("now", (LongSupplier)() -> 1L);

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
        
        PublisherBase.interval(30, TimeUnit.MILLISECONDS, exec).flatMap(v -> 
                PublisherBase.<String>fromIterable(Arrays.asList("A"))
                .flatMap(w -> PublisherBase.<List<Integer>>fromCallable(() -> Arrays.asList(1, 2)).subscribeOn(exec).flatMap(PublisherBase::fromIterable)))
        .subscribe(ts);
        
        Thread.sleep(5000);
        
        ts.cancel();
        
        ts.assertNoError();
    }
}
