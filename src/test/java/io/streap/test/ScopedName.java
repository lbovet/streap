package io.streap.test;

/**
 * Create a name from the current method name.
 */
public class ScopedName {

    public static String get() {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        return stackTrace[1].getMethodName();
    }

    public static String get(String name) {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        return stackTrace[1].getMethodName()+"."+name;
    }
}
