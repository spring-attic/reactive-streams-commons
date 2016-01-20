package reactivestreams.commons.util;

import java.lang.reflect.Constructor;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

public class ConstructorTestBuilderTest {
    static class A {
        public A(Object o, int i) {
            
        }
    }

    static class B {
        public B(Object o, int i) {
            Objects.requireNonNull(o, "o");
        }
    }
    
    @Test
    public void compileWithParamNames() throws Exception {
        Constructor<?> c = A.class.getConstructor(Object.class, Integer.TYPE);
        
        Assert.assertTrue("You must enable parameter name retention via javac command line -parameters", c.getParameters()[0].isNamePresent());
        Assert.assertEquals("You must enable parameter name retention via javac command line -parameters", "o", c.getParameters()[0].getName());
    }

    @Test
    public void noRefNullCheck() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(A.class);
        ctb.addRef("o", new Object());
        ctb.addInt("i", 0, 1);
        
        try {
            ctb.test();
            Assert.fail("Did not trigger validation failures");
        } catch (AssertionError ex) {
            // expected
        }
    }

    @Test
    public void noRangeCheck() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(B.class);
        ctb.addRef("o", new Object());
        ctb.addInt("i", 0, 1);
        
        try {
            ctb.test();
            Assert.fail("Did not trigger validation failures");
        } catch (AssertionError ex) {
            // expected
        }
    }
}
