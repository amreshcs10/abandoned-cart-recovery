package com.example.cartrecovery.config;

import com.example.cartrecovery.model.Arm;

import java.util.List;

/**
 * Every tunable in one place. All durations are in seconds.
 * Reminder offsets are measured from the cart's LAST ACTIVITY (the event time of the last
 * add/edit/resume). With the defaults the first reminder goes out at the moment the cart becomes
 * abandoned (30 min idle), then 1 h and 24 h after last activity.
 */
public record Config(
        long inactivityWindowS,
        List<Long> reminderOffsetsS,
        int maxAttempts,          // total send attempts per reminder
        long retryBaseS,          // backoff = base * 2^(attempt-1), capped at retryMaxS
        long retryMaxS,
        long maxLatenessS,        // a reminder later than this is dropped (EXPIRED)
        long leaseS,              // un-acked timers are redelivered after this
        int maxRemindersPerCart,  // across all abandonment episodes of one cart
        String experimentSalt,
        List<Arm> arms) {

    public static final long MINUTE = 60;
    public static final long HOUR = 60 * MINUTE;
    public static final long DAY = 24 * HOUR;

    public Config {
        reminderOffsetsS = List.copyOf(reminderOffsetsS);
        arms = List.copyOf(arms);
        validateOffsets("default", reminderOffsetsS, inactivityWindowS);
        int total = arms.stream().mapToInt(Arm::weight).sum();
        if (total != 100) throw new IllegalArgumentException("arm weights must sum to 100, got " + total);
        for (Arm a : arms) {
            if (a.offsets() != null) validateOffsets(a.name(), a.offsets(), inactivityWindowS);
        }
    }

    private static void validateOffsets(String who, List<Long> offs, long window) {
        for (int i = 1; i < offs.size(); i++) {
            if (offs.get(i) <= offs.get(i - 1)) {
                throw new IllegalArgumentException(who + ": reminder offsets must be strictly increasing");
            }
        }
        if (!offs.isEmpty() && offs.get(0) < window) {
            throw new IllegalArgumentException(who + ": first reminder offset must be >= inactivity window");
        }
    }

    public List<Long> offsetsFor(Arm arm) {
        return arm.offsets() != null ? arm.offsets() : reminderOffsetsS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Config defaults() {
        return builder().build();
    }

    public static final class Builder {
        private long inactivityWindowS = 30 * MINUTE;
        private List<Long> reminderOffsetsS = List.of(30 * MINUTE, HOUR, DAY);
        private int maxAttempts = 5;
        private long retryBaseS = 30;
        private long retryMaxS = 15 * MINUTE;
        private long maxLatenessS = 2 * HOUR;
        private long leaseS = 60;
        private int maxRemindersPerCart = 3;
        private String experimentSalt = "cart-reminders-v1";
        private List<Arm> arms = List.of(Arm.control(10), Arm.treatment("standard", 90));

        public Builder inactivityWindowS(long v) { inactivityWindowS = v; return this; }
        public Builder reminderOffsetsS(Long... v) { reminderOffsetsS = List.of(v); return this; }
        public Builder maxAttempts(int v) { maxAttempts = v; return this; }
        public Builder retryBaseS(long v) { retryBaseS = v; return this; }
        public Builder retryMaxS(long v) { retryMaxS = v; return this; }
        public Builder maxLatenessS(long v) { maxLatenessS = v; return this; }
        public Builder leaseS(long v) { leaseS = v; return this; }
        public Builder maxRemindersPerCart(int v) { maxRemindersPerCart = v; return this; }
        public Builder experimentSalt(String v) { experimentSalt = v; return this; }
        public Builder arms(Arm... v) { arms = List.of(v); return this; }

        public Config build() {
            return new Config(inactivityWindowS, reminderOffsetsS, maxAttempts, retryBaseS, retryMaxS,
                    maxLatenessS, leaseS, maxRemindersPerCart, experimentSalt, arms);
        }
    }
}
