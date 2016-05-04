package rsc.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import rsc.scheduler.ParallelScheduler.ParallelWorker;

public class ParallelSchedulerTest {

    ParallelScheduler scheduler;
    
    @Before
    public void before() {
        scheduler = new ParallelScheduler();
    }
    
    @After
    public void after() {
        scheduler.shutdown();
    }
    
    @Test(timeout = 1000)
    public void direct() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        
        Runnable r = () -> counter.getAndIncrement();
        
        int n = 10_000;
        
        for (int i = 0; i < n; i++) {
            scheduler.schedule(r);
        }
        
        Thread.sleep(10);
        
        while (counter.get() != n) ;
        
        Thread.sleep(10);
        
        Assert.assertEquals(n, counter.get());
    }

    @Test
    public void worker() throws Exception {
        for (int j = 0; j < 100; j++) {
            AtomicInteger counter = new AtomicInteger();
            
            CountDownLatch cdl = new CountDownLatch(1);
            
            Runnable r = () -> counter.getAndIncrement();
            
            int n = 100_000;
            
            Scheduler.Worker w = scheduler.createWorker();
            try {
                for (int i = 0; i < n; i++) {
                    w.schedule(r);
                }
                w.schedule(cdl::countDown);
                
                if (!cdl.await(5, TimeUnit.SECONDS)) {
                    Assert.fail("Timed out: " + counter.get() + " of " + n + " @ " + j);
                }
                
                Assert.assertEquals(n, counter.get());
                
                Thread.sleep(10);
                
                Assert.assertEquals(0, ((ParallelWorker)w).pendingTasks());
            } finally {
                w.shutdown();
            }
        }
    }

}
