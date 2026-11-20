package com.example.cartrecovery.model;

/**
 * A cart event as published by the Cart Service. {@code seq} is a per-cart, strictly increasing
 * revision stamped by the Cart Service; it makes duplicate and out-of-order delivery harmless.
 */
public record CartEvent(String eventId, String cartId, long seq, EventType type, long occurredAt,
                        String userId, String sku, int qty, String name, double price) {}
