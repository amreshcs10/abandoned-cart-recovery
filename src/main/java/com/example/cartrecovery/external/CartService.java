package com.example.cartrecovery.external;

import com.example.cartrecovery.model.CartEvent;
import com.example.cartrecovery.model.EventType;

import java.util.HashMap;
import java.util.Map;

/**
 * Fake of the existing Cart Service: builds cart events and stamps each with an event id and the
 * per-cart {@code seq}. (Production publishes through a transactional outbox.)
 */
public final class CartService {
    private final Map<String, Long> seq = new HashMap<>();
    private long n;

    private CartEvent event(String cartId, EventType type, long at, String userId, String sku, int qty,
                            String name, double price) {
        long s = seq.merge(cartId, 1L, Long::sum);
        return new CartEvent("e" + (++n), cartId, s, type, at, userId, sku, qty, name, price);
    }

    public CartEvent add(String cartId, long at, String userId) {
        return add(cartId, at, userId, "sku-1", 1, 10.0);
    }

    public CartEvent add(String cartId, long at, String userId, String sku, int qty, double price) {
        return event(cartId, EventType.ITEM_ADDED, at, userId, sku, qty, sku, price);
    }

    public CartEvent addNamed(String cartId, long at, String userId, String name, double price) {
        return event(cartId, EventType.ITEM_ADDED, at, userId, "sku-1", 1, name, price);
    }

    public CartEvent update(String cartId, long at, String sku, int qty) {
        return event(cartId, EventType.ITEM_UPDATED, at, null, sku, qty, sku, 10.0);
    }

    public CartEvent remove(String cartId, long at, String sku) {
        return event(cartId, EventType.ITEM_REMOVED, at, null, sku, 0, sku, 0);
    }

    public CartEvent clear(String cartId, long at) {
        return event(cartId, EventType.CART_CLEARED, at, null, null, 0, null, 0);
    }

    public CartEvent checkout(String cartId, long at) {
        return event(cartId, EventType.CHECKOUT_COMPLETED, at, null, null, 0, null, 0);
    }

    public CartEvent resume(String cartId, long at) {
        return event(cartId, EventType.CART_RESUMED, at, null, null, 0, null, 0);
    }

    public CartEvent merged(String cartId, long at) {
        return event(cartId, EventType.CART_MERGED, at, null, null, 0, null, 0);
    }
}
