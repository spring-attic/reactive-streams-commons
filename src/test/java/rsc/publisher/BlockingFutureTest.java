package rsc.publisher;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import rsc.processor.SimpleProcessor;

public class BlockingFutureTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new BlockingFuture<>(null);
    }

    @Test(expected = NullPointerException.class)
    public void futureDefaultNull() {
        PublisherEmpty.instance().toFuture(null);
    }

    @Test(expected = NullPointerException.class)
    public void completableFutureDefaultNull() {
        PublisherEmpty.instance().toCompletableFuture(null);
    }
    
    @Test(timeout = 2000)
    public void normal() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toFuture();
            
            exec.schedule(() -> { sp.onNext(1); sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Assert.assertEquals((Integer)1, f.get());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void empty() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toFuture();
            
            exec.schedule(() -> { sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);

            try {
                Integer v = f.get();
                Assert.fail("Failed to throw ExecutionException and returned a value: " + v);
            } catch (ExecutionException ex) {
                if (!(ex.getCause() instanceof NoSuchElementException)) {
                    throw ex;
                }
            }
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void emptyDefault() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toFuture(1);
            
            exec.schedule(() -> { sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Assert.assertEquals((Integer)1, f.get());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void error() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toFuture();
            
            exec.schedule(() -> { sp.onError(new RuntimeException("forced failure")); }, 500, TimeUnit.MILLISECONDS);

            try {
                Integer v = f.get();
                Assert.fail("Failed to throw ExecutionException and returned a value: " + v);
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (!(cause instanceof RuntimeException) || !"forced failure".equals(cause.getMessage())) {
                    throw ex;
                }
            }
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void normalCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture();
            
            exec.schedule(() -> { sp.onNext(1); sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Assert.assertEquals((Integer)1, f.get());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void emptyCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture();
            
            exec.schedule(() -> { sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);

            try {
                Integer v = f.get();
                Assert.fail("Failed to throw ExecutionException and returned a value: " + v);
            } catch (ExecutionException ex) {
                if (!(ex.getCause() instanceof NoSuchElementException)) {
                    throw ex;
                }
            }
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void emptyDefaultCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture(1);
            
            exec.schedule(() -> { sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Assert.assertEquals((Integer)1, f.get());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void errorCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture();
            
            exec.schedule(() -> { sp.onError(new RuntimeException("forced failure")); }, 500, TimeUnit.MILLISECONDS);

            try {
                Integer v = f.get();
                Assert.fail("Failed to throw ExecutionException and returned a value: " + v);
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (!(cause instanceof RuntimeException) || !"forced failure".equals(cause.getMessage())) {
                    throw ex;
                }
            }
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void normalCancelled() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toFuture();
            
            Future<?> g = exec.schedule(() -> { sp.onNext(1); sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Thread.sleep(100);
            
            f.cancel(true);

            g.get();
            
            Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void normalCancelledCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture();
            
            Future<?> g = exec.schedule(() -> { sp.onNext(1); sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Thread.sleep(100);
            
            f.cancel(true);

            g.get();
            
            Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
            
        } finally {
            exec.shutdown();
        }
    }

    @Test(timeout = 2000)
    public void composedCompletable() throws Exception {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        try {
            SimpleProcessor<Integer> sp = new SimpleProcessor<>();
            
            Future<Integer> f = sp.toCompletableFuture().thenApply(v -> v + 1);
            
            exec.schedule(() -> { sp.onNext(1); sp.onComplete(); }, 500, TimeUnit.MILLISECONDS);
            
            Assert.assertEquals((Integer)2, f.get());
            
        } finally {
            exec.shutdown();
        }
    }

}
