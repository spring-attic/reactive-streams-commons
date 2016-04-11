package reactivestreams.commons.util;

import java.util.concurrent.*;

import org.junit.*;

import reactivestreams.commons.scheduler.Scheduler;

public class ExecutorSchedulerTest {

    ExecutorService exec;
    
    @After
    public void after() {
        if (exec != null) {
            exec.shutdown();
        }
    }

    void runNormalDirect(Scheduler s) throws Exception {
        int n = 1000;
        CountDownLatch cdl = new CountDownLatch(n);
        
        Runnable task = cdl::countDown;
        
        for (int i = 0; i < n; i++) {
            s.schedule(task);
        }
        
        Assert.assertTrue("Didn't execute all tasks: " + cdl.getCount(), cdl.await(5, TimeUnit.SECONDS));
    }
    
    @Test
    public void normalDirectNonTrampolined() throws Exception {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, false);

        runNormalDirect(s);
    }

    @Test
    public void normalDirectTrampolined() throws Exception {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, true);
        
        runNormalDirect(s);
    }

    void runNormalWorker(Scheduler s) throws Exception {
        int m = 50;
        for (int j = 0; j < m; j++) {
            
            Scheduler.Worker w = s.createWorker();
            
            int n = 1000;
            CountDownLatch cdl = new CountDownLatch(n);
            
            Runnable task = cdl::countDown;
            
            for (int i = 0; i < n; i++) {
                w.schedule(task);
            }
            
            Assert.assertTrue("Didn't execute all tasks: " + cdl.getCount(), cdl.await(5, TimeUnit.SECONDS));
            
            w.shutdown();
        }
    }
    
    @Test
    public void normalWorkerNonTrampolined() throws Exception {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, false);
        
        runNormalWorker(s);
    }

    @Test
    public void normalWorkerTrampolined() throws Exception {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, true);

        runNormalWorker(s);
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
    
    @Test
    public void verifyTrampoline() throws Exception {
        exec = Executors.newFixedThreadPool(2);
        Scheduler s = new ExecutorScheduler(exec, true);
        
        Scheduler.Worker w = s.createWorker();

        ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<>();
        CountDownLatch cdl = new CountDownLatch(2);
        
        w.schedule(() -> {
            sleep(250);
            q.offer(1);
            cdl.countDown();
        });
        
        w.schedule(() -> {
            q.offer(2);
            cdl.countDown();
        });
        
        Assert.assertTrue(cdl.await(5, TimeUnit.SECONDS));
        
        Assert.assertEquals((Integer)1, q.poll());
        Assert.assertEquals((Integer)2, q.poll());
    }
    
    @Test
    public void postAfterSchedulerShutdownNonTrampolined() {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, false);
        
        Scheduler.Worker w = s.createWorker();

        w.shutdown();
        
        Runnable r = w.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);
    }

    @Test
    public void postAfterSchedulerShutdownTrampolined() {
        exec = Executors.newSingleThreadExecutor();
        Scheduler s = new ExecutorScheduler(exec, true);
        
        Scheduler.Worker w = s.createWorker();

        w.shutdown();
        
        Runnable r = w.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);
    }

    @Test
    public void postAfterExecutorShutdownNonTrampolined() {
        exec = Executors.newSingleThreadExecutor();
        exec.shutdown();
        Scheduler s = new ExecutorScheduler(exec, false);

        Runnable r = s.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);

        Scheduler.Worker w = s.createWorker();
        
        r = w.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);
    }

    @Test
    public void postAfterExecutorShutdownTrampolined() {
        exec = Executors.newSingleThreadExecutor();
        exec.shutdown();
        Scheduler s = new ExecutorScheduler(exec, true);
        
        Runnable r = s.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);

        Scheduler.Worker w = s.createWorker();
        
        r = w.schedule(() -> { });
        
        Assert.assertSame(Scheduler.REJECTED, r);
    }

}
