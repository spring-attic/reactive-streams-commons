
package rsc.state;

/**
 * A component that has introspection options
 */
public interface Introspectable {

    /**
     * A component that is meant to be introspectable on finest logging level
     */
    int TRACE_ONLY = 0b00000001;

    /**
     * A component that is meant to be embedded or gating linked upstream(s) and/or downstream(s) components
     */
    int INNER = 0b00000010;

    /**
     * A component that is intended to build others
     */
    int FACTORY = 0b00000100;

    /**
     * Flags determining the nature of this {@link Introspectable}, can be a combination of those, e.g. :
     * <pre>
     *     int mode = Introspectable.LOGGING | Introspectable.FACTORY
     * @return the introspection mode, see constants
     */
    default int getMode(){
        return 0;
    }

    /**
     * The name of the component
     * @return the name of the component
     */
    default String getName() {
        return getClass().getSimpleName();
    }


    /**
     * Defined identifier or null if not available
     * @return defined identifier or null if not available
     */
    default Object key() {
        return null;
    }

    /**
     * Current error if any, default to null
     * @return Current error if any, default to null
     */
    default Throwable getError(){
        return null;
    }

}
