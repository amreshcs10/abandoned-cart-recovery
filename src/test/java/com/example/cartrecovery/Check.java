package com.example.cartrecovery;

import java.util.Objects;

/** Minimal assertions, so the scenarios run with only a JDK. */
final class Check {
    private Check() {}

    static void equal(Object expected, Object actual) {
        equal(expected, actual, null);
    }

    static void equal(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError((message == null ? "" : message + ": ")
                    + "expected <" + expected + "> but was <" + actual + ">");
        }
    }

    static void equal(long expected, long actual) {
        equal(expected, actual, null);
    }

    static void equal(long expected, long actual, String message) {
        equal((Object) expected, (Object) actual, message);
    }

    static void isTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void rejects(Runnable action) {
        try {
            action.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("expected IllegalArgumentException");
    }
}
