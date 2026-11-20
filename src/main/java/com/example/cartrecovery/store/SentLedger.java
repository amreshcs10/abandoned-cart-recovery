package com.example.cartrecovery.store;

import java.util.HashMap;
import java.util.Map;

/**
 * One row per reminder slot, keyed {@code cartId:v{version}:r{idx}}. Production: DynamoDB with
 * conditional writes and a 30-day TTL. {@code SENT} is terminal.
 */
public final class SentLedger {

    public enum Status {
        /** Claimed; a send may or may not have reached the provider. */
        PENDING,
        SENT,
        FAILED
    }

    private final Map<String, Status> rows = new HashMap<>();

    public Status get(String key) {
        return rows.get(key);
    }

    public void set(String key, Status status) {
        if (rows.get(key) == Status.SENT) return; // SENT is terminal
        rows.put(key, status);
    }
}
