package com.example.cartrecovery.store;

import com.example.cartrecovery.model.Timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Durable delay queue. Production: Redis sorted sets (or a DynamoDB time index).
 * {@link #schedule} is an UPSERT by timer id. The DETECT timer for a cart has a fixed id,
 * so each edit moves the one existing timer instead of piling up stale ones (ZADD).
 * {@link #poll} LEASES due timers. A timer not acked before its lease runs out is handed
 * out again (at-least-once; covers worker crashes).
 * {@link #ack} is compare-and-delete on version + attempt, so a worker finishing an old
 * lease cannot delete a timer that was re-armed meanwhile.
 */
public final class TimerStore {
    private enum State { READY, INFLIGHT }

    private record Entry(long t, long tick, String id) {}

    private final long leaseS;
    private final Map<String, Timer> timers = new HashMap<>();
    private final Map<String, State> state = new HashMap<>();
    private final Map<String, Long> leaseUntil = new HashMap<>();
    private final PriorityQueue<Entry> heap = new PriorityQueue<>(
            (a, b) -> a.t != b.t ? Long.compare(a.t, b.t) : Long.compare(a.tick, b.tick));
    private long tick;

    public TimerStore(long leaseS) {
        this.leaseS = leaseS;
    }

    private void push(long t, String id) {
        heap.add(new Entry(t, ++tick, id));
    }

    public void schedule(Timer timer) {
        timers.put(timer.id(), timer);
        state.put(timer.id(), State.READY);
        leaseUntil.remove(timer.id());
        push(timer.fireAt(), timer.id());
    }

    private void reap(long now) {
        List<String> expired = new ArrayList<>();
        leaseUntil.forEach((id, until) -> { if (until <= now) expired.add(id); });
        for (String id : expired) {
            long until = leaseUntil.remove(id);
            state.put(id, State.READY);
            push(until, id);
        }
    }

    /** Live = the entry still refers to a ready timer at (or after) its current fire time. */
    private boolean live(Entry e) {
        Timer t = timers.get(e.id);
        return t != null && state.get(e.id) == State.READY && e.t >= t.fireAt();
    }

    public List<Timer> poll(long now) {
        reap(now);
        List<Timer> due = new ArrayList<>();
        while (!heap.isEmpty() && heap.peek().t <= now) {
            Entry e = heap.poll();
            if (!live(e)) continue; // lazy deletion of superseded entries
            state.put(e.id, State.INFLIGHT);
            leaseUntil.put(e.id, now + leaseS);
            due.add(timers.get(e.id));
        }
        return due;
    }

    public void ack(Timer timer) {
        Timer cur = timers.get(timer.id());
        if (cur == null || cur.version() != timer.version() || cur.attempt() != timer.attempt()) {
            return; // re-armed while we held the old lease; leave the new one alone
        }
        if (state.get(timer.id()) != State.INFLIGHT) return;
        timers.remove(timer.id());
        state.remove(timer.id());
        leaseUntil.remove(timer.id());
    }

    /** Earliest time anything becomes due (a ready timer or a lease expiry), or null. */
    public Long nextTime() {
        while (!heap.isEmpty() && !live(heap.peek())) heap.poll();
        Long best = heap.isEmpty() ? null : heap.peek().t;
        for (long until : leaseUntil.values()) {
            if (best == null || until < best) best = until;
        }
        return best;
    }

    public int pending() {
        return timers.size();
    }
}
