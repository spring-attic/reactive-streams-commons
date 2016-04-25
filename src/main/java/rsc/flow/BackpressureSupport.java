package rsc.flow;

import java.lang.annotation.*;

/**
 * Indicates what kind of backpressure behavior the operator has its
 * input and output side, or optionally as an inner receiver or
 * producer of Publishers.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface BackpressureSupport {
    /**
     * Returns the main input's backpressure mode.
     * @return the main input's backpressure mode
     */
    BackpressureMode input();
    
    /**
     * Returns the main output's backpressure mode.
     * @return the main output's backpressure mode
     */
    BackpressureMode output();
    
    /**
     * Returns the inner input's backpressure mode in
     * case the operator consumes some Publisher.
     * @return the inner input's backpressure mode
     */
    BackpressureMode innerInput() default BackpressureMode.NOT_APPLICABLE;
    
    /**
     * Returns the inner output's backpressure mode in
     * case the operator produces some Publisher.
     * @return the inner output's backpressure mode
     */
    BackpressureMode innerOutput() default BackpressureMode.NOT_APPLICABLE;
}
