package reactivestreams.commons.publisher;

import java.util.concurrent.*;

import org.junit.*;

import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.support.ConstructorTestBuilder;

public class PublisherFutureTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherFuture.class);
        
        ctb.addRef("future", new FutureTask<>(() -> 1));
        ctb.addLong("timeout", Long.MIN_VALUE, Long.MAX_VALUE);
        ctb.addRef("unit", TimeUnit.SECONDS);
    }
    
    @Test(timeout = 2000)
    public void normal() {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            Future<Integer> f = exec.schedule(() -> 1, 500, TimeUnit.MILLISECONDS);
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            new PublisherFuture<>(f).subscribe(ts);
            
            ts.await();
            
            ts.assertValue(1)
            .assertNoError()
            .assertComplete();
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void normalBackpressured() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            Future<Integer> f = exec.schedule(() -> 1, 100, TimeUnit.MILLISECONDS);
            
            TestSubscriber<Integer> ts = new TestSubscriber<>(0);
            
            new PublisherFuture<>(f).subscribe(ts);
            
            Thread.sleep(500);
            
            ts.request(1);
            
            ts.await();
            
            ts.assertValue(1)
            .assertNoError()
            .assertComplete();
            
        } finally {
            exec.shutdown();
        }
    }
    
    @Test(timeout = 2000)
    public void timeout() {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            Future<Integer> f = exec.schedule(() -> 1, 1500, TimeUnit.MILLISECONDS);
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            new PublisherFuture<>(f, 500, TimeUnit.MILLISECONDS).subscribe(ts);
            
            ts.await();
            
            ts.assertNoValues()
            .assertError(TimeoutException.class)
            .assertNotComplete();
            
        } finally {
            exec.shutdown();
        }
    }
    
    @Test(timeout = 2000)
    public void futureThrows() {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            Future<Integer> f = exec.schedule(() -> { throw new RuntimeException("forced failure"); }, 500, TimeUnit.MILLISECONDS);
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            new PublisherFuture<>(f).subscribe(ts);
            
            ts.await();
            
            ts.assertNoValues()
            .assertError(ExecutionException.class)
            .assertErrorCause(RuntimeException.class)
            .assertNotComplete();
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void noCrossCancel() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
        try {
            Future<Integer> f = exec.schedule(() -> 1, 1500, TimeUnit.MILLISECONDS);
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            exec.submit(() -> new PublisherFuture<>(f).subscribe(ts));
            
            Thread.sleep(500);
            
            ts.cancel();

            Thread.sleep(100);
            
            Assert.assertFalse("Future done?", f.isDone());
            
            Assert.assertFalse("Future cancelled?", f.isCancelled());
            
            ts.assertNoValues()
            .assertNoError()
            .assertNotComplete();
            
        } finally {
            exec.shutdown();
        }
    }

}
