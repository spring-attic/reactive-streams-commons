package reactivestreams.commons.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import reactivestreams.commons.flow.Cancellation;
import reactivestreams.commons.scheduler.Scheduler;

public class WorkerSchedulerTest {
    @Test
    public void independentShutdown() {
        
        ExecutorScheduler main = new ExecutorScheduler(Runnable::run);
        
        WorkerScheduler ws = new WorkerScheduler(main);
        
        Scheduler.Worker w1 = ws.createWorker();
        Scheduler.Worker w2 = ws.createWorker();
        
        AtomicInteger a1 = new AtomicInteger();
        AtomicInteger a2 = new AtomicInteger();
        AtomicInteger a3 = new AtomicInteger();
        
        w1.schedule(a1::getAndIncrement);
        w1.shutdown();
        
        w2.schedule(a2::getAndIncrement);
        w2.shutdown();

        Cancellation c3 = w2.schedule(a3::getAndIncrement);
        
        Assert.assertEquals(1, a1.get());
        Assert.assertEquals(1, a2.get());
        Assert.assertEquals(0, a3.get());
        Assert.assertSame(Scheduler.REJECTED, c3);
        
        ws.shutdown();
    }
}
