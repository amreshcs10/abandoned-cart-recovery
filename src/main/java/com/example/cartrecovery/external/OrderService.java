package com.example.cartrecovery.external;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fake of the existing Order Service: the source of truth for purchases, queried right before a
 * send. Guards against a CHECKOUT_COMPLETED event that is still in flight (lag, partition
 * rebalance) when a reminder comes due.
 */
public final class OrderService {
    private final Map<String, List<Long>> orders = new HashMap<>();

    public void record(String cartId, long at) {
        orders.computeIfAbsent(cartId, k -> new ArrayList<>()).add(at);
    }

    public boolean hasOrderSince(String cartId, long since, long now) {
        return orders.getOrDefault(cartId, List.of()).stream().anyMatch(t -> since <= t && t <= now);
    }
}
