package com.example.cartrecovery.model;

import java.util.List;

/**
 * One experiment arm. {@code send=false} is the holdout: abandonment is detected and logged as an
 * exposure, but nothing is scheduled. {@code offsets=null} means "use the configured defaults".
 */
public record Arm(String name, int weight, boolean send, List<Long> offsets, String template) {

    public static Arm treatment(String name, int weight) {
        return new Arm(name, weight, true, null, "default");
    }

    public static Arm control(int weight) {
        return new Arm("control", weight, false, null, "default");
    }

    public static Arm withOffsets(String name, int weight, String template, Long... offsets) {
        return new Arm(name, weight, true, List.of(offsets), template);
    }
}
