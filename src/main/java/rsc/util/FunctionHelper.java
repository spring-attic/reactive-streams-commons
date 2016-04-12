package rsc.util;

import java.util.function.Function;

/**
 * Utility methods to work with functions and other lambda types.
 */
public enum FunctionHelper {
    ;
    
    static final Function<Object, Object> IDENTITY = new Function<Object, Object>() {
        @Override
        public Object apply(Object t) {
            return t;
        }
    };
    
    /**
     * Returns the identity function.
     * @param <T> the input and output type
     * @return the identity function instance
     */
    @SuppressWarnings("unchecked")
    public static <T> Function<T, T> identity() {
        return (Function<T, T>)IDENTITY;
    }
}
