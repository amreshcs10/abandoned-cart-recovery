package com.example.cartrecovery.model;

public record Item(String name, int qty, double price) {
    public double lineValue() {
        return price * qty;
    }
}
