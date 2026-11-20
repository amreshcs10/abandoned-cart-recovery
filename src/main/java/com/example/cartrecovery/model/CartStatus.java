package com.example.cartrecovery.model;

public enum CartStatus {
    ACTIVE, ABANDONED, PURCHASED, CLEARED, MERGED;

    /** A closed cart is never reopened; the Cart Service issues a new cartId. */
    public boolean closed() {
        return this == PURCHASED || this == CLEARED || this == MERGED;
    }
}
