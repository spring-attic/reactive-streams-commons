package rsc.documentation;

import java.lang.annotation.*;

/**
 * Specifies the fusion mode the operators supports externally or
 * internally if it takes or produces Publishers.
 * <p>
 * Defaults for input and output is NONE and for
 * innerInput and innerOutput is NOT_APPLICABLE.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface FusionSupport {
    /**
     * Returns the fusion mode support by the main input.
     * @return the fusion mode support by the main input
     */
    FusionMode[] input() default FusionMode.NONE;
    
    /**
     * Returns the fusion mode support by the main output.
     * @return the fusion mode support by the main output
     */
    FusionMode[] output() default FusionMode.NONE;
    
    /**
     * Returns the fusion mode support by the inner input.
     * @return the fusion mode support by the inner input
     */
    FusionMode[] innerInput() default FusionMode.NOT_APPLICABLE;
    
    /**
     * Returns the fusion mode support by the inner output.
     * @return the fusion mode support by the inner output
     */
    FusionMode[] innerOutput() default FusionMode.NOT_APPLICABLE;
}
