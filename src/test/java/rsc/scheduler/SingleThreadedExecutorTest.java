package rsc.scheduler;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rsc.publisher.Px;
import rsc.test.TestSubscriber;

public class SingleThreadedExecutorTest {

    @Test
    public void range() {
        int count = 1_000_000;
        Integer[] array = new Integer[count];
        for (int i = 0; i < count; i++) {
            array[i] = i;
        }
        
        Scheduler s1 = new SingleScheduler2();
        Scheduler s2 = new SingleScheduler2();
        
        TestSubscriber<Integer> ts = Px.fromArray(array).subscribeOn(s1).observeOn(s2).test();
        
        ts.await(5, TimeUnit.SECONDS);
        ts.assertValueCount(count)
        .assertNoError()
        .assertComplete();
    }
}
