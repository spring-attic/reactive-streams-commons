package rsc.documentation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Indicates what kind of behaviors the operator has.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Operator {
    /**
     * Returns the operator behaviors
     * @return the operator behaviors
     */
    OperatorType[] traits();

    /**
     * Returns the operator known API aliases
     * @return the operator known API aliases
     */
    String[] aliases() default "";


}
