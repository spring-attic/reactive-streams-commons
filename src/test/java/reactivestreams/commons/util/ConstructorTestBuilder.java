package reactivestreams.commons.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Test constructor parameters
 */
public final class ConstructorTestBuilder {
    
    final Class<?> clazz;
    
    final Map<String, Object> parameters;
    
    public ConstructorTestBuilder(Class<?> clazz) {
        this.clazz = Objects.requireNonNull(clazz, "clazz");
        parameters = new HashMap<>();
    }
    
    public void addRef(String name, Object validValue) {
        parameters.put(name, new ConstructorRefParameter(name, validValue));
    }
    
    public void addInt(String name, int minAllowed, int maxAllowed) {
        parameters.put(name, new ConstructorIntParameter(name, minAllowed, maxAllowed));
    }
    
    public void addLong(String name, long minAllowed, long maxAllowed) {
        parameters.put(name, new ConstructorLongParameter(name, minAllowed, maxAllowed));
    }
    
    public void test() {
        for (Constructor<?> c : clazz.getConstructors()) {

            Parameter[] params = c.getParameters();
            int n = params.length;
            if (n == 0) {
                continue;
            }
            
            // null checks
            for (int i = 0; i < n; i++) {
                Object[] paramValues = new Object[n];
                
                boolean hasNullParams = false;
                
                for (int j = 0; j < n; j++) {
                    
                    String pname = params[j].getName();
                    
                    Object cp = parameters.get(pname);
                    
                    if (cp == null) {
                        throw new IllegalStateException("Constructor " + c + " parameter " + pname + " is missing a test setting");
                    }
                    
                    if (cp instanceof ConstructorRefParameter) {
                        if (i == j) {
                            hasNullParams = true;
                            paramValues[j] = null;
                        } else {
                            paramValues[j] = ((ConstructorRefParameter)cp).validValue;
                        }
                    } else
                    if (cp instanceof ConstructorIntParameter) {
                        paramValues[j] = ((ConstructorIntParameter)cp).minAllowed;
                    } else 
                    if (cp instanceof ConstructorLongParameter) {
                        paramValues[j] = ((ConstructorLongParameter)cp).minAllowed;
                    } else {
                        throw new IllegalStateException("Unsupported ConstructorXParameter: " + cp);
                    }
                }
                
                if (hasNullParams) {
                    try {
                        c.newInstance(paramValues);
                        throw new AssertionError("Constructor " + c + " parameter " + params[i] + " didn't throw");
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException ex) {
                        throw new AssertionError(ex);
                    } catch (InvocationTargetException ex) {
                        Throwable cause = ex.getCause();
                        if (!(cause instanceof NullPointerException)) {
                            throw new AssertionError("Constructor " + c + " parameter " + params[i], ex);
                        }
                    }
                }
            }

            // argument range
            for (int i = 0; i < n; i++) {
                Object[] paramValues = new Object[n];
                
                boolean hasInvalidParams = false;
                
                for (int j = 0; j < n; j++) {
                    
                    String pname = params[j].getName();
                    
                    Object cp = parameters.get(pname);
                    
                    if (cp == null) {
                        throw new IllegalStateException("Constructor " + c + " parameter " + pname + " is missing a test setting");
                    }
                    
                    if (cp instanceof ConstructorRefParameter) {
                        paramValues[j] = ((ConstructorRefParameter)cp).validValue;
                    } else
                    if (cp instanceof ConstructorIntParameter) {
                        ConstructorIntParameter cpi = (ConstructorIntParameter)cp;
                        if (i == j) {
                            if (cpi.minAllowed > Integer.MIN_VALUE) {
                                hasInvalidParams = true;
                                paramValues[j] = cpi.minAllowed - 1;
                            } else
                            if (cpi.maxAllowed < Integer.MAX_VALUE) {
                                hasInvalidParams = true;
                                paramValues[j] = cpi.maxAllowed + 1;
                            } else {
                                paramValues[j] = cpi.minAllowed;
                            }
                        } else {
                            paramValues[j] = cpi.minAllowed;
                        }
                    } else 
                    if (cp instanceof ConstructorLongParameter) {
                        ConstructorLongParameter cpi = (ConstructorLongParameter)cp;
                        if (i == j) {
                            if (cpi.minAllowed > Long.MIN_VALUE) {
                                hasInvalidParams = true;
                                paramValues[j] = cpi.minAllowed - 1;
                            } else
                            if (cpi.maxAllowed < Long.MAX_VALUE) {
                                hasInvalidParams = true;
                                paramValues[j] = cpi.maxAllowed + 1;
                            } else {
                                paramValues[j] = cpi.minAllowed;
                            }
                        } else {
                            paramValues[j] = cpi.minAllowed;
                        }
                    } else {
                        throw new IllegalStateException("Unsupported ConstructorXParameter: " + cp);
                    }
                }
                
                if (hasInvalidParams) {
                    try {
                        c.newInstance(paramValues);
                        throw new AssertionError("Constructor " + c + " parameter " + params[i] + " didn't throw");
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException ex) {
                        throw new AssertionError(ex);
                    } catch (InvocationTargetException ex) {
                        Throwable cause = ex.getCause();
                        if (!(cause instanceof IllegalArgumentException)) {
                            throw new AssertionError("Constructor " + c + " parameter " + params[i], ex);
                        }
                    }
                }
            }

        }
    }
    
    static final class ConstructorRefParameter {
        final String name;
        final Object validValue;

        public ConstructorRefParameter(String name, Object validValue) {
            this.name = name;
            this.validValue = validValue;
        }
    }
    
    static final class ConstructorIntParameter {
        final String name;
        final int minAllowed;
        final int maxAllowed;

        public ConstructorIntParameter(String name, int minAllowed, int maxAllowed) {
            this.name = name;
            this.minAllowed = minAllowed;
            this.maxAllowed = maxAllowed;
        }
        
        
    }
    
    static final class ConstructorLongParameter {
        final String name;
        final long minAllowed;
        final long maxAllowed;
        
        public ConstructorLongParameter(String name, long minAllowed, long maxAllowed) {
            this.name = name;
            this.minAllowed = minAllowed;
            this.maxAllowed = maxAllowed;
        }
    }
}
