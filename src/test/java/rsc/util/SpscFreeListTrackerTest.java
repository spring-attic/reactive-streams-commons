package rsc.util;

import org.junit.Test;

public class SpscFreeListTrackerTest {
    static final class Tracker extends SpscFreeListTracker<Object> {
        static final Object[] EMPTY = new Object[0];
        static final Object[] TERMINATED = new Object[0];
        @Override
        protected Object[] empty() {
            return EMPTY;
        }
        
        @Override
        protected Object[] newArray(int size) {
            return new Object[size];
        }
        
        @Override
        protected Object[] terminated() {
            return TERMINATED;
        }
        
        @Override
        protected void unsubscribeEntry(Object entry) {
            
        }
        
        @Override
        protected void setIndex(Object entry, int index) {
            
        }
    }

    @Test
    public void addUntilGrows() {
        Tracker tr = new Tracker();
        for (int j = 0; j < 16; j++) {
            for (int i = 0; i < 16; i++) {
                tr.add(i);
            }
            
            for (int i = 0; i < 16; i++) {
                tr.remove(i);
            }
        }
    }
}
