package com.example.cartrecovery.model;

import java.util.EnumSet;
import java.util.Set;

public enum EventType {
    ITEM_ADDED,
    ITEM_UPDATED,
    ITEM_REMOVED,
    CART_CLEARED,
    CHECKOUT_COMPLETED,
    /** Shopper came back: viewed the cart or began checkout. */
    CART_RESUMED,
    /** Guest cart merged into a user cart at login. */
    CART_MERGED;

    public static final Set<EventType> EDITS = EnumSet.of(ITEM_ADDED, ITEM_UPDATED, ITEM_REMOVED);
}
