package com.example.cartrecovery.clock;

/** Manually advanced clock, so days of simulated time run in milliseconds and tests are exact. */
public final class FakeClock implements Clock {
    private long now;

    public FakeClock(long start) {
        this.now = start;
    }

    @Override
    public long now() {
        return now;
    }

    public void set(long t) {
        if (t < now) throw new IllegalArgumentException("clock cannot go backwards");
        now = t;
    }
}
