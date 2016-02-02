package reactivestreams.commons.publisher;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherIntervalTest {

    @Test
    public void constructors() {
        
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherInterval.class);
        
        ctb.addLong("initialDelay", Long.MIN_VALUE, Long.MAX_VALUE);
        ctb.addLong("period", 0, Long.MAX_VALUE);
        ctb.addRef("unit", TimeUnit.SECONDS);
        ctb.addRef("executor", exec);
        ctb.addRef("asyncExecutor", (BiFunction<Runnable, Long, Runnable>)(r, a) -> r);
        ctb.addRef("now", (LongSupplier)() -> 1L);
        
        exec.shutdown();
    }
    
    @Test
    public void normal() {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
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
}
