package rsc.scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rsc.scheduler.TimedScheduler.TimedWorker;

public class CompositeTimedWorkerTest {

    TimedScheduler timer;

    TimedWorker worker;
    
    @Before
    public void before() {
        timer = new SingleTimedScheduler();
        
        worker = timer.createWorker();
    }
    
    @After
    public void after() {
        worker.shutdown();
        
        timer.shutdown();
    }
    
    @Test
    public void independentWorkers() throws InterruptedException {
        TimedWorker w1 = new CompositeTimedWorker(worker);
        
        TimedWorker w2 = new CompositeTimedWorker(worker);
        
        CountDownLatch cdl = new CountDownLatch(1);
        
        w1.shutdown();
        
        try {
            w1.schedule(() -> { });
            Assert.fail("Failed to reject task");
        } catch (Throwable ex) {
            // ingoring
        }
        
        w2.schedule(cdl::countDown);
        
        if (!cdl.await(1, TimeUnit.SECONDS)) {
            Assert.fail("Worker 2 didn't execute in time");
        }
        w2.shutdown();
    }

    @Test
    public void massCancel() throws InterruptedException {
        TimedWorker w1 = new CompositeTimedWorker(worker);
        
        AtomicInteger counter = new AtomicInteger();
        
        Runnable task = counter::getAndIncrement;
        
        for (int i = 0; i < 10; i++) {
            w1.schedule(task, 500, TimeUnit.MILLISECONDS);
        }
        
        w1.shutdown();
        
        Thread.sleep(1000);
        
        Assert.assertEquals(0, counter.get());
    }

}
