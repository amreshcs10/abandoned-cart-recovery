package com.example.cartrecovery.model;

/**
 * A durable timer.
 *
 * @param version cart version this timer was created for (the fence)
 * @param idx     reminder index within the episode (-1 for DETECT)
 * @param dueAt   original scheduled time, kept across retries (for lateness)
 */
public record Timer(String id, Kind kind, String cartId, long fireAt, long version, int idx,
                    int attempt, long dueAt) {

    public enum Kind {
        /** "Has this cart been idle for the whole window?" */
        DETECT,
        /** "Send reminder #idx for this abandonment episode." */
        REMINDER
    }

    /** One DETECT timer per cart: a fixed id, so each edit moves it instead of adding another. */
    public static String detectId(String cartId) {
        return cartId + ":detect";
    }

    /** Ledger row and provider idempotency key for one reminder slot of one episode. */
    public static String reminderKey(String cartId, long version, int idx) {
        return cartId + ":v" + version + ":r" + idx;
    }

    public static Timer detect(String cartId, long version, long due) {
        return new Timer(detectId(cartId), Kind.DETECT, cartId, due, version, -1, 1, due);
    }

    public static Timer reminder(String cartId, long version, int idx, long due) {
        return new Timer(reminderKey(cartId, version, idx), Kind.REMINDER, cartId, due, version, idx, 1, due);
    }

    public String key() {
        return reminderKey(cartId, version, idx);
    }

    /** Next attempt of this reminder; keeps the original due time. */
    public Timer retryAt(long fireAt) {
        int next = attempt + 1;
        return new Timer(key() + ":a" + next, kind, cartId, fireAt, version, idx, next, dueAt);
    }
}
