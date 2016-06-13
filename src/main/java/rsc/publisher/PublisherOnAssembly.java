package rsc.publisher;

import org.reactivestreams.*;

import rsc.flow.Fuseable;
import rsc.subscriber.*;
import rsc.util.ExceptionHelper;

/**
 * Captures the current stacktrace when this publisher is created and
 * makes it available/visible for debugging purposes from
 * the inner Subscriber.
 * <p>
 * Note that getting a stacktrace is a costly operation.
 * <p>
 * The operator sanitizes the stacktrace and removes noisy entries such as:
 * <ul>
 * <li>java.lang.Thread entries</li>
 * <li>method references with source line of 1 (bridge methods)</li>
 * <li>Tomcat worker thread entries</li>
 * <li>JUnit setup</li>
 * </ul>
 * 
 * @param <T> the value type passing through
 */
public final class PublisherOnAssembly<T> extends PublisherSource<T, T> implements Fuseable {

    final String stacktrace;
    
    /**
     * If set to true, the creation of PublisherOnAssembly will capture the raw
     * stacktrace instead of the sanitized version.
     */
    public static volatile boolean fullStackTrace;

    public PublisherOnAssembly(Publisher<? extends T> source) {
        super(source);
        this.stacktrace = takeStacktrace();
    }

    private String takeStacktrace() {
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();

        StringBuilder sb = new StringBuilder("Assembly trace:\n");
        
        for (StackTraceElement e : stes) {
            String row = e.toString();
            if (!fullStackTrace) {
                if (e.getLineNumber() <= 1) {
                    continue;
                }
                if (row.contains("Px.onAssembly")) {
                    continue;
                }
                if (row.contains("PublisherOnAssembly.")) {
                    continue;
                }
                if (row.contains(".junit.runner")) {
                    continue;
                }
                if (row.contains(".junit4.runner")) {
                    continue;
                }
                if (row.contains(".junit.internal")) {
                    continue;
                }
                if (row.contains("sun.reflect")) {
                    continue;
                }
                if (row.contains("java.lang.Thread.")) {
                    continue;
                }
                if (row.contains("ThreadPoolExecutor")) {
                    continue;
                }
                if (row.contains("org.apache.catalina.")) {
                    continue;
                }
                if (row.contains("org.apache.tomcat.")) {
                    continue;
                }
            }
            sb.append(row).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Returns the stacktrace as captured when this PublisherOnAssembly has been instantiated.
     * @return the stacktrace
     */
    public String stacktrace() {
        return stacktrace;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (s instanceof Fuseable.ConditionalSubscriber) {
            Fuseable.ConditionalSubscriber<? super T> cs = (Fuseable.ConditionalSubscriber<? super T>) s;
            source.subscribe(new OnAssemblyConditionalSubscriber<>(cs, stacktrace));
        } else {
            source.subscribe(new OnAssemblySubscriber<>(s, stacktrace));
        }
    }
    
    /**
     * The holder for the assembly stacktrace (as its message).
     */
    public static final class OnAssemblyException extends RuntimeException {
        
        /** */
        private static final long serialVersionUID = 5278398300974016773L;

        public OnAssemblyException(String message) {
            super(message);
        }
        
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
    
    static final class OnAssemblySubscriber<T> extends BasicFuseableSubscriber<T, T> {
        final String stacktrace;
        
        public OnAssemblySubscriber(Subscriber<? super T> actual, String stacktrace) {
            super(actual);
            this.stacktrace = stacktrace;
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            t.addSuppressed(new OnAssemblyException(stacktrace));
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            return transitiveAnyFusion(requestedMode);
        }
        
        @Override
        public boolean isEmpty() {
            try {
                return qs.isEmpty();
            } catch (final Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                ex.addSuppressed(new OnAssemblyException(stacktrace));
                throw ex;
            }
        }
        
        @Override
        public T poll() {
            try {
                return qs.poll();
            } catch (final Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                ex.addSuppressed(new OnAssemblyException(stacktrace));
                throw ex;
            }
        }
    }
    
    static final class OnAssemblyConditionalSubscriber<T> extends BasicFuseableConditionalSubscriber<T, T> {
        final String stacktrace;
        
        public OnAssemblyConditionalSubscriber(ConditionalSubscriber<? super T> actual, String stacktrace) {
            super(actual);
            this.stacktrace = stacktrace;
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public boolean tryOnNext(T t) {
            return actual.tryOnNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            t.addSuppressed(new OnAssemblyException(stacktrace));
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            return transitiveAnyFusion(requestedMode);
        }
        
        @Override
        public boolean isEmpty() {
            try {
                return qs.isEmpty();
            } catch (final Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                ex.addSuppressed(new OnAssemblyException(stacktrace));
                throw ex;
            }
        }
        
        @Override
        public T poll() {
            try {
                return qs.poll();
            } catch (final Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                ex.addSuppressed(new OnAssemblyException(stacktrace));
                throw ex;
            }
        }
    }
}
