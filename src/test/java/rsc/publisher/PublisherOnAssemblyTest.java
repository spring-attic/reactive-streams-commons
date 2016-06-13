package rsc.publisher;

import java.util.Arrays;

import org.junit.Test;
import org.junit.Assert;

import rsc.test.TestSubscriber;

public class PublisherOnAssemblyTest {

    @Test
    public void checkRecorded() {
        boolean ta = Px.trackAssembly;
        try {
            Px.trackAssembly = true;
            
            TestSubscriber<Object> ts = new TestSubscriber<>();
            
            Px.error(new Exception()).subscribe(ts);
            
            ts.assertError(Exception.class);
            
            Throwable e = ts.errors().get(0);
            
            Throwable[] suppressed = e.getSuppressed();
            
            Assert.assertEquals(Arrays.toString(suppressed), 1, suppressed.length);
            
            String stacktrace = suppressed[0].getMessage();
            
            Assert.assertTrue(stacktrace, stacktrace.contains("checkRecorded") 
                    && stacktrace.contains("PublisherOnAssemblyTest"));
            
        } finally {
            Px.trackAssembly = ta;
        }
    }
    
    @Test
    public void checkNotRecorded() {
        boolean ta = Px.trackAssembly;
        try {
            Px.trackAssembly = false;
            
            TestSubscriber<Object> ts = new TestSubscriber<>();
            
            Px.error(new Exception()).subscribe(ts);
            
            ts.assertError(Exception.class);
            
            Throwable e = ts.errors().get(0);
            
            Throwable[] suppressed = e.getSuppressed();
            
            Assert.assertEquals(Arrays.toString(suppressed), 0, suppressed.length);
        } finally {
            Px.trackAssembly = ta;
        }
    }
}
