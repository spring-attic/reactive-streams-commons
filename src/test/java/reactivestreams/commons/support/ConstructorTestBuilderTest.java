package reactivestreams.commons.support;

import java.util.Objects;

import org.junit.*;

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
