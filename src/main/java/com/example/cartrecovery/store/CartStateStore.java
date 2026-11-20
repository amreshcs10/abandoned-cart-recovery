package com.example.cartrecovery.store;

import com.example.cartrecovery.model.CartState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Cart state keyed by cartId. Production: DynamoDB or Redis with one conditional write per event
 * ({@code ... WHERE last_seq < :seq}), so applying an event is atomic.
 */
public final class CartStateStore {
    private final Map<String, CartState> carts = new HashMap<>();

    public CartState get(String cartId) {
        return carts.get(cartId);
    }

    public CartState getOrCreate(String cartId) {
        return carts.computeIfAbsent(cartId, CartState::new);
    }

    public Collection<CartState> all() {
        return carts.values();
    }
}
