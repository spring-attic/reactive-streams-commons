package rsc.util;

/**
 * Utility class to compute the next power of two.
 */
public enum PowerOf2 {
    ;
    
    public static int roundUp(int x) {
        return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
    }
}
