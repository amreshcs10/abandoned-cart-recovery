package com.example.cartrecovery;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Runs every {@link Scenario} with only a JDK (no JUnit, no Maven). Used by run.sh / run.cmd.
 * Exits non-zero if any scenario fails.
 */
public final class PipelineTests {

    /** Every scenario class, in report order. Add new scenario classes here. */
    static final List<Class<?>> SCENARIO_CLASSES = List.of(
            DetectionScenarios.class,
            CancellationScenarios.class,
            IdempotencyScenarios.class,
            FailureHandlingScenarios.class,
            SecondaryScenarios.class,
            SoakScenarios.class);

    /** A named, runnable scenario. */
    record Case(String name, Runnable body) {}

    static List<Case> cases() {
        List<Case> out = new ArrayList<>();
        for (Class<?> cls : SCENARIO_CLASSES) {
            List<Method> methods = new ArrayList<>();
            for (Method m : cls.getDeclaredMethods()) if (m.isAnnotationPresent(Scenario.class)) methods.add(m);
            methods.sort(Comparator.comparing(Method::getName));
            for (Method m : methods) {
                out.add(new Case(cls.getSimpleName() + "." + m.getName(), () -> invoke(cls, m)));
            }
        }
        return out;
    }

    private static void invoke(Class<?> cls, Method m) {
        try {
            m.invoke(cls.getDeclaredConstructor().newInstance());
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IllegalStateException(cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) {
        int passed = 0;
        List<String> failed = new ArrayList<>();
        for (Case c : cases()) {
            try {
                c.body().run();
                passed++;
                System.out.println("  ok    " + c.name());
            } catch (Throwable t) {
                failed.add(c.name());
                System.out.println("  FAIL  " + c.name() + ": " + t);
            }
        }
        System.out.println(passed + " passed, " + failed.size() + " failed");
        if (!failed.isEmpty()) System.exit(1);
    }
}
