package reactivestreams.commons.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class BlockingIterableTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(BlockingIterable.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addLong("batchSize", 1, Long.MAX_VALUE);
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test(timeout = 5000)
    public void normal() {
        List<Integer> values = new ArrayList<>();
        
        for (Integer i : Px.range(1, 10).toIterable()) {
            values.add(i);
        }
        
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
    }

    @Test(timeout = 5000)
    public void empty() {
        List<Integer> values = new ArrayList<>();
        
        for (Integer i : PublisherEmpty.<Integer>instance().toIterable()) {
            values.add(i);
        }
        
        Assert.assertEquals(Collections.emptyList(), values);
    }
    
    @Test(timeout = 5000, expected = RuntimeException.class)
    public void error() {
        List<Integer> values = new ArrayList<>();
        
        for (Integer i : new PublisherError<Integer>(new RuntimeException("forced failure")).toIterable()) {
            values.add(i);
        }
        
        Assert.assertEquals(Collections.emptyList(), values);
    }

    @Test(timeout = 5000)
    public void stream() {
        List<Integer> values = new ArrayList<>();

        Px.range(1, 10).stream().forEach(values::add);

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
    }

    @Test(timeout = 5000)
    public void streamEmpty() {
        List<Integer> values = new ArrayList<>();

        PublisherEmpty.<Integer>instance().stream().forEach(values::add);

        Assert.assertEquals(Collections.emptyList(), values);
    }

    @Test(timeout = 5000)
    public void streamLimit() {
        List<Integer> values = new ArrayList<>();

        Px.range(1, Integer.MAX_VALUE).stream().limit(10).forEach(values::add);

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
    }
    @Test(timeout = 5000)
    public void streamParallel() {
        int n = 1_000_000;

        Optional<Integer> opt = Px.range(1, n).parallelStream().max(Integer::compare);

        Assert.assertTrue("No maximum?", opt.isPresent());
        Assert.assertEquals((Integer)n, opt.get());
    }
}
