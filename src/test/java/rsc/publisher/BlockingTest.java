package rsc.publisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockingTest {

    static ExecutorService exec;

    @BeforeClass
    public static void before() {
        exec = Executors.newSingleThreadExecutor();
    }
    
    @AfterClass
    public static void after() {
        exec.shutdown();
    }
    
    @Test
    public void peekLastSync() {
        Assert.assertEquals((Integer)10, Px.range(1, 10).peekLast());
    }
    
    @Test
    public void blockingFirst() {
        Assert.assertEquals((Integer)1, Px.range(1, 10).observeOn(exec).blockingFirst());
    }

    @Test
    public void blockingLast() {
        Assert.assertEquals((Integer)10, Px.range(1, 10).observeOn(exec).blockingLast());
    }
}
