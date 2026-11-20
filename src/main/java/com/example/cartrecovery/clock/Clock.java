package com.example.cartrecovery.clock;

/** The only source of time for every component. Times are epoch seconds. */
public interface Clock {
    long now();
}
