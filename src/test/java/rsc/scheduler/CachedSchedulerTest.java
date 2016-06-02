package rsc.scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import rsc.scheduler.Scheduler.Worker;

public class CachedSchedulerTest {
    
    CachedScheduler scheduler;
    
    @Before
    public void before() {
        scheduler = new CachedScheduler();
    }
    
    @After
    public void after() {
        scheduler.shutdown();
    }
    
    @Test
    public void workerTasks() throws Exception {
        int n = 10000;
        
        CountDownLatch cdl = new CountDownLatch(n);
        
        Worker w = scheduler.createWorker();
        
        try {
            for (int i = 0; i < n; i++) {
                Assert.assertNotSame(Scheduler.REJECTED, w.schedule(cdl::countDown));
            }
            
            cdl.await(5, TimeUnit.SECONDS);
        } finally {
            w.shutdown();
        }
        
        Assert.assertEquals(0, cdl.getCount());
        
        Assert.assertSame(Scheduler.REJECTED, w.schedule(() -> { }));
    }

    @Test
    public void directTasks() throws Exception {
        int n = 100;
        
        CountDownLatch cdl = new CountDownLatch(n);
        
        for (int i = 0; i < n; i++) {
            Assert.assertNotSame(Scheduler.REJECTED, scheduler.schedule(cdl::countDown));
        }
        
        cdl.await(5, TimeUnit.SECONDS);
        
        Assert.assertEquals(0, cdl.getCount());
    }
    
    @Test
    public void reuseThreadPool() throws Exception {
        String[] threadName = { "", "" };
        Worker w = scheduler.createWorker();
        try {
            CountDownLatch cdl = new CountDownLatch(1);
            
            w.schedule(() -> {
                threadName[0] = Thread.currentThread().getName();
                cdl.countDown();
            });
            
            if (!cdl.await(5, TimeUnit.SECONDS)) {
                Assert.fail("The first task didn't run in time");
            }
        } finally {
            w.shutdown();
        }
        
        w = scheduler.createWorker();
        try {
            CountDownLatch cdl = new CountDownLatch(1);
            
            w.schedule(() -> {
                threadName[1] = Thread.currentThread().getName();
                cdl.countDown();
            });
            
            if (!cdl.await(5, TimeUnit.SECONDS)) {
                Assert.fail("The second task didn't run in time");
            }
        } finally {
            w.shutdown();
        }
        
        Assert.assertEquals(threadName[0], threadName[1]);
    }
}
