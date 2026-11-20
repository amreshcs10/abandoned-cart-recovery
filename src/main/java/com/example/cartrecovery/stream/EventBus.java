package com.example.cartrecovery.stream;

import com.example.cartrecovery.model.CartEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * The {@code cart-events} topic (Kafka, keyed by cartId). Each event becomes visible to the
 * consumer at its arrival time, which may be later than when it happened (lag) and may repeat
 * (redelivery). Events with the same arrival time come out in publish order.
 */
public final class EventBus {
    private record Pending(long arriveAt, long n, CartEvent event) {}

    private final PriorityQueue<Pending> queue = new PriorityQueue<>(
            (a, b) -> a.arriveAt != b.arriveAt ? Long.compare(a.arriveAt, b.arriveAt) : Long.compare(a.n, b.n));
    private long n;

    /** The event reaches the consumer when it happens. */
    public void publish(CartEvent event) {
        publish(event, event.occurredAt());
    }

    /** The event reaches the consumer at {@code arriveAt}: models broker or consumer lag. */
    public void publish(CartEvent event, long arriveAt) {
        queue.add(new Pending(arriveAt, ++n, event));
    }

    /** Arrival time of the next event, or null. */
    public Long nextArrival() {
        return queue.isEmpty() ? null : queue.peek().arriveAt;
    }

    /** Every event that has arrived by {@code now}, in arrival order. */
    public List<CartEvent> pollArrived(long now) {
        List<CartEvent> out = new ArrayList<>();
        while (!queue.isEmpty() && queue.peek().arriveAt <= now) out.add(queue.poll().event);
        return out;
    }
}
