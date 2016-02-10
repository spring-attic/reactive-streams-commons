package reactivestreams.commons.util;

import org.junit.*;

public class SpscLinkedArrayQueueTest {

    SpscLinkedArrayQueue<Integer> queue;
    
    @Before
    public void before() {
        queue = new SpscLinkedArrayQueue<>(16);
    }
    
    @Test
    public void offerTakeOneByOne() {
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(0, queue.size());

        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(queue.offer(i));
            Assert.assertFalse(queue.isEmpty());
            Assert.assertEquals(1, queue.size());
            
            Assert.assertEquals((Integer)i, queue.peek());
            Assert.assertEquals((Integer)i, queue.poll());
            Assert.assertTrue(queue.isEmpty());
            Assert.assertEquals(0, queue.size());
        }
    }
    
    @Test
    public void grow() {
        
        for (int i = 0; i < 32; i++) {
            Assert.assertTrue(queue.offer(i));
            Assert.assertFalse(queue.isEmpty());
            Assert.assertEquals(1 + i, queue.size());
        }

        for (int i = 0; i < 32; i++) {
            Assert.assertEquals((Integer)i, queue.peek());
            Assert.assertEquals((Integer)i, queue.poll());
        }

        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(0, queue.size());
    }
}
