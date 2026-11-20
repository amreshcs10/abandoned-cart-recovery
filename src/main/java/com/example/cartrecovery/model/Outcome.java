package com.example.cartrecovery.model;

import java.util.EnumSet;
import java.util.Set;

/** One decision the pipeline made. Feeds metrics, experiment analysis and the verifier. */
public record Outcome(long t, String cartId, Type type, int idx, long version, String detail) {

    public enum Type {
        // detection
        ACTIVITY, ABANDONED, HELD_OUT_CONTROL, REMINDER_SCHEDULED, DETECT_STALE,
        CLOSED_PURCHASED, CLOSED_CLEARED, CLOSED_MERGED,
        EVENT_IGNORED_STALE_OR_DUP, EVENT_IGNORED_CLOSED,
        // dispatch
        SENT, CANCELLED_STALE, BLOCKED_BY_ORDER, EXPIRED, SUPPRESSED_UNREACHABLE, SUPPRESSED_CAP,
        DUPLICATE_SKIPPED, RETRY_SCHEDULED, DEAD_LETTERED, WORKER_CRASHED, DLQ_REPLAYED;

        private static final Set<Type> TERMINAL = EnumSet.of(SENT, CANCELLED_STALE, BLOCKED_BY_ORDER, EXPIRED,
                SUPPRESSED_UNREACHABLE, SUPPRESSED_CAP, DUPLICATE_SKIPPED, DEAD_LETTERED);

        /** Every scheduled reminder must end in exactly one of these. */
        public boolean terminal() {
            return TERMINAL.contains(this);
        }

        public static Type closed(CartStatus status) {
            return valueOf("CLOSED_" + status.name());
        }
    }

    public String key() {
        return Timer.reminderKey(cartId, version, idx);
    }
}
