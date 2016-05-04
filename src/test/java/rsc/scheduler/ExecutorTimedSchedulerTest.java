package rsc.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import rsc.scheduler.TimedScheduler.TimedWorker;

public class ExecutorTimedSchedulerTest {

    static ScheduledExecutorService executor;
    
    static TimedScheduler scheduler;
    
    TimedWorker worker;
    
    @BeforeClass
    public static void beforeClass() {
        executor = Executors.newScheduledThreadPool(3);
        scheduler = new ExecutorTimedScheduler(executor);
    }
    
    @AfterClass
    public static void afterClass() {
        executor.shutdown();
    }
    
    @Before
    public void before() {
        worker = scheduler.createWorker();
    }
    
    @After
    public void after() {
        worker.shutdown();
    }
    
    @Test
    public void fifo() throws Exception {
        Queue<Integer> queue = new ConcurrentLinkedQueue<>();
        
        CountDownLatch cdl = new CountDownLatch(1);
        
        int n = 10_000;
        
        for (int i = 0; i < n; i++) {
            int j = i;
            worker.schedule(() -> queue.offer(j));
        }
        worker.schedule(cdl::countDown);
        
        if (!cdl.await(5, TimeUnit.SECONDS)) {
            Assert.fail("Timeout " + queue.size());
        }
        
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(i, queue.poll().intValue());
        }        
    }

    @Test
    public void delayed() throws Exception {
        Queue<Integer> queue = new ConcurrentLinkedQueue<>();
        
        CountDownLatch cdl = new CountDownLatch(1);
        
        int n = 10_000;
        
        for (int i = 0; i < n; i++) {
            int j = i;
            worker.schedule(() -> queue.offer(j), 100, TimeUnit.MILLISECONDS);
        }
        worker.schedule(cdl::countDown, 250, TimeUnit.MILLISECONDS);
        
        if (!cdl.await(5, TimeUnit.SECONDS)) {
            Assert.fail("Timeout " + queue.size());
        }
        
        // requires single-threaded executor because same-delay may be picked up by
        // multiple threads and then switch order.
        Set<Integer> set = new HashSet<>(queue);
        Assert.assertEquals(n, set.size());
    }

    @Test
    public void fifoPeriodic() throws Exception {
        Queue<Integer> queue = new ConcurrentLinkedQueue<>();
        
        CountDownLatch cdl = new CountDownLatch(1);
        
        int n = 1000;

        worker.schedulePeriodically(new Runnable() {
            int i;
            @Override
            public void run() {
                queue.offer(i++);
                if (i == n) {
                    worker.shutdown();
                    cdl.countDown();
                }
            }
        }, 1, 1, TimeUnit.MILLISECONDS);

        if (!cdl.await(5, TimeUnit.SECONDS)) {
            Assert.fail("Timeout: " + queue.size());
        }
        
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(i, queue.poll().intValue());
        }        
    }

    @Test
    public void shutdown() throws Exception {
        
        AtomicInteger count = new AtomicInteger();
        
        worker.schedule(() -> count.getAndIncrement(), 1, TimeUnit.SECONDS);
        worker.schedulePeriodically(() -> count.getAndIncrement(), 1, 1, TimeUnit.SECONDS);
        
        worker.shutdown();
        
        Thread.sleep(1500);
        
        Assert.assertEquals(0, count.get());
    }
}
