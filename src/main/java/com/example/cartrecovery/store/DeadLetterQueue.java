package com.example.cartrecovery.store;

import com.example.cartrecovery.model.Timer;

import java.util.ArrayList;
import java.util.List;

/** Reminders that failed permanently or exhausted their retries. Production: SQS or a Kafka DLQ topic. */
public final class DeadLetterQueue {

    public record DeadLetter(Timer timer, String reason, long at) {}

    private List<DeadLetter> items = new ArrayList<>();

    public void put(Timer timer, String reason, long at) {
        items.add(new DeadLetter(timer, reason, at));
    }

    public List<DeadLetter> items() {
        return List.copyOf(items);
    }

    /** Take everything out, for replay. */
    public List<DeadLetter> drain() {
        List<DeadLetter> out = items;
        items = new ArrayList<>();
        return out;
    }
}
