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
import reactivestreams.commons.support.ConstructorTestBuilder;

public class BlockingStreamTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(BlockingStream.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addLong("batchSize", 1, Long.MAX_VALUE);
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }

    @Test(timeout = 1000)
    public void stream() {
        List<Integer> values = new ArrayList<>();
        
        new PublisherRange(1, 10).stream().forEach(values::add);
        
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
    }

    @Test(timeout = 1000)
    public void streamEmpty() {
        List<Integer> values = new ArrayList<>();
        
        PublisherEmpty.<Integer>instance().stream().forEach(values::add);
        
        Assert.assertEquals(Collections.emptyList(), values);
    }
    
    @Test(timeout = 1000)
    public void streamLimit() {
        List<Integer> values = new ArrayList<>();
        
        new PublisherRange(1, Integer.MAX_VALUE).stream().limit(10).forEach(values::add);
        
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
    }
    @Test(timeout = 1000)
    public void streamParallel() {
        int n = 1_000_000;
        
        Optional<Integer> opt = new PublisherRange(1, n).parallelStream().max(Integer::compare);

        Assert.assertTrue("No maximum?", opt.isPresent());
        Assert.assertEquals((Integer)n, opt.get());
    }
}
