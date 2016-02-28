
package reactivestreams.commons.state;

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
     *
     * @return the introspection mode, see constants
     */
    default int getMode(){
        return 0;
    }

    /**
     *
     * @return the name of the operator
     */
    default String getName() {
        return getClass().getSimpleName();
    }

}
