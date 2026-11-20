package com.example.cartrecovery.model;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Everything the pipeline knows about one cart. */
public final class CartState {
    public final String cartId;
    public String userId;
    public CartStatus status = CartStatus.ACTIVE;
    /** Bumped on every applied shopper event: the fencing token for timers. */
    public long version;
    public long lastSeq;
    public long lastActivity;
    public final Map<String, Item> items = new LinkedHashMap<>();
    public String arm;
    public int episodes;
    /**
     * Reminder keys that may have reached the shopper (claimed and not known to have failed).
     * Counting at claim time, not at SENT, means a crash between the provider call and the ack can
     * never let the per-cart cap be exceeded.
     */
    public final Set<String> counted = new HashSet<>();

    public CartState(String cartId) {
        this.cartId = cartId;
    }

    public int remindersSent() {
        return counted.size();
    }

    /** Only identified shoppers can be messaged. */
    public boolean contactable() {
        return userId != null;
    }
}
