package rsc.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import rsc.flow.Cancellation;
import rsc.scheduler.Scheduler;

public final class TestHelper {
    private TestHelper() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * Runs two actions concurrently, one in the current thread and the other on
     * the IO scheduler, synchronizing their execution as much as possible; rethrowing
     * any exceptions they produce.
     * <p>This helper waits until both actions have run or times out in 5 seconds.
     * @param r1 the first action
     * @param r2 the second action
     * @param scheduler the target scheduler to use
     */
    public static void race(Runnable r1, final Runnable r2, Scheduler scheduler) {
        final AtomicInteger counter = new AtomicInteger(2);
        final Throwable[] errors = { null, null };
        final CountDownLatch cdl = new CountDownLatch(1);
        
            Cancellation c = scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    if (counter.decrementAndGet() != 0) {
                        while (counter.get() != 0 && Thread.currentThread().isInterrupted());
                    }
                    
                    if (!Thread.currentThread().isInterrupted()) {
                        try {
                            r2.run();
                        } catch (Throwable ex) {
                            errors[1] = ex;
                        }
                    }
                    
                    cdl.countDown();
                }
            });

            if (counter.decrementAndGet() != 0) {
                while (counter.get() != 0);
            }
            
            try {
                r1.run();
            } catch (Throwable ex) {
                errors[0] = ex;
            }
            
            List<Throwable> errorList = new ArrayList<>();
            
            try {
                if (!cdl.await(5, TimeUnit.SECONDS)) {
                    errorList.add(new TimeoutException());
                    c.dispose();
                }
            } catch (InterruptedException ex) {
                errorList.add(ex);
            }
            
            if (errors[0] != null) {
                errorList.add(errors[0]);
            }

            if (errors[1] != null) {
                errorList.add(errors[1]);
            }

            if (!errorList.isEmpty()) {
                if (errorList.size() == 1) {
                    ExceptionHelper.propagate(errorList.get(0));
                } else {
                    RuntimeException ex = new RuntimeException("Multiple errors");
                    for (Throwable e : errorList) {
                        ex.addSuppressed(e);
                    }
                    throw ex;
                }
            }
    }
}
