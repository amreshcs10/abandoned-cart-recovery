package com.example.cartrecovery.verify;

import com.example.cartrecovery.Pipeline;
import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.external.NotificationProvider.Delivery;
import com.example.cartrecovery.external.NotificationProvider.Fault;
import com.example.cartrecovery.external.NotificationProvider.FaultPlan;
import com.example.cartrecovery.model.CartEvent;
import com.example.cartrecovery.model.CartState;
import com.example.cartrecovery.model.EventType;
import com.example.cartrecovery.model.Outcome;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;

import static com.example.cartrecovery.config.Config.DAY;
import static com.example.cartrecovery.config.Config.HOUR;
import static com.example.cartrecovery.config.Config.MINUTE;

/**
 * Randomised soak with fault injection, checked against safety and liveness invariants.
 * Injects duplicate events, late/out-of-order events, transient and permanent send failures, and
 * worker crashes between send and ack.
 */
public final class Soak {
    private Soak() {}

    public record Report(int carts, int events, double wallSeconds, Map<String, Long> counts,
                         List<String> violations) {
        public boolean ok() { return violations.isEmpty(); }

        public long count(String k) { return counts.getOrDefault(k, 0L); }
    }

    /** Deterministic per-(seed, key, n) uniform in [0,1). */
    private static double u(Object... parts) {
        return new SplittableRandom((long) Objects.hash(parts) * 0x9E3779B97F4A7C15L).nextDouble();
    }

    private static long exp(Random rng, double mean) {
        return Math.round(-mean * Math.log(1 - rng.nextDouble()));
    }

    private static long uniform(Random rng, double lo, double hi) {
        return Math.round(lo + (hi - lo) * rng.nextDouble());
    }

    public static Report run(int nCarts, long seed, Config config) {
        Random rng = new Random(seed);
        FaultPlan faults = (key, n) -> {
            double r = u("fault", seed, key, n);
            return r < 0.05 ? Fault.TRANSIENT : r < 0.055 ? Fault.PERMANENT : null;
        };
        Pipeline p = new Pipeline(config, 0, faults, (key, n) -> n == 1 && u("crash", seed, key) < 0.01, Map.of());
        CartService cs = new CartService();
        Map<String, List<CartEvent>> eventsByCart = new HashMap<>();
        Set<String> delayed = new HashSet<>();
        Map<String, Long> checkoutAt = new HashMap<>();
        int nEvents = 0;

        for (int i = 0; i < nCarts; i++) {
            String cid = "c" + i;
            String uid = rng.nextDouble() < 0.9 ? "u" + (i % (nCarts / 2)) : null; // some users have 2 carts
            List<CartEvent> evs = shopperBehaviour(rng, cs, cid, uid);
            for (CartEvent ev : evs) {
                eventsByCart.computeIfAbsent(cid, k -> new ArrayList<>()).add(ev);
                if (ev.type() == EventType.CHECKOUT_COMPLETED) {
                    checkoutAt.put(cid, ev.occurredAt());
                    p.orders.record(cid, ev.occurredAt()); // order service knows at once
                }
                double d = rng.nextDouble();
                long lag = d < 0.90 ? 0 : d < 0.99 ? uniform(rng, 0, 120) : uniform(rng, 10 * MINUTE, 40 * MINUTE);
                if (lag > 0) delayed.add(cid);
                p.ingest(ev, ev.occurredAt() + lag);
                nEvents++;
                if (rng.nextDouble() < 0.05) { // broker redelivery
                    p.ingest(ev, ev.occurredAt() + lag + uniform(rng, 0, 300));
                    nEvents++;
                }
            }
        }

        long start = System.nanoTime();
        p.runUntil(6 * HOUR + 3 * DAY);
        double wall = (System.nanoTime() - start) / 1e9;

        Map<String, Long> counts = new TreeMap<>();
        for (Outcome o : p.outcomes.all()) counts.merge(o.type().name(), 1L, Long::sum);
        counts.put("provider_dedup_hits", (long) p.provider.providerDedupHits);
        counts.put("dlq_size", (long) p.dlq.items().size());
        counts.put("delivered", (long) p.provider.inbox.size());
        return new Report(nCarts, nEvents, wall, counts, checkInvariants(p, config, eventsByCart, delayed, checkoutAt));
    }

    /** Add, a few edits, then checkout (30%), clear (5%), resume then checkout (10%), or walk away. */
    private static List<CartEvent> shopperBehaviour(Random rng, CartService cs, String cid, String uid) {
        long t = uniform(rng, 0, 6 * HOUR);
        List<CartEvent> evs = new ArrayList<>();
        evs.add(cs.add(cid, t, uid, "s0", 1, new double[]{5, 20, 80}[rng.nextInt(3)]));
        int edits = new int[]{0, 0, 1, 2, 3}[rng.nextInt(5)];
        for (int k = 0; k < edits; k++) {
            t += exp(rng, 15 * MINUTE);
            evs.add(rng.nextDouble() < 0.6 ? cs.add(cid, t, uid, "s" + (k + 1), 1, 10)
                    : cs.update(cid, t, "s0", 1 + rng.nextInt(4)));
        }
        double r = rng.nextDouble();
        t += exp(rng, 40 * MINUTE);
        if (r < 0.30) {
            evs.add(cs.checkout(cid, t));
        } else if (r < 0.35) {
            evs.add(cs.clear(cid, t));
        } else if (r < 0.45) {
            evs.add(cs.resume(cid, t));
            t += exp(rng, 10 * MINUTE);
            evs.add(cs.checkout(cid, t));
        }
        return evs;
    }

    private static List<String> checkInvariants(Pipeline p, Config config, Map<String, List<CartEvent>> eventsByCart,
                                                Set<String> delayed, Map<String, Long> checkoutAt) {
        List<String> v = new ArrayList<>();

        // S1: never deliver after a purchase.
        for (Delivery d : p.provider.inbox.values()) {
            Long co = checkoutAt.get(cartOf(d.key()));
            if (co != null && d.at() >= co) v.add("S1 delivered " + d.key() + " at " + d.at() + " after checkout " + co);
        }

        // S2: at most one SENT per idempotency key; cap respected per cart.
        Map<String, Integer> sent = new HashMap<>();
        for (Outcome o : p.outcomes.all()) {
            if (o.type() == Outcome.Type.SENT) sent.merge(o.key(), 1, Integer::sum);
        }
        sent.forEach((k, c) -> { if (c > 1) v.add("S2 duplicate SENT " + k); });
        Map<String, Integer> perCart = new HashMap<>();
        for (String k : p.provider.inbox.keySet()) perCart.merge(cartOf(k), 1, Integer::sum);
        perCart.forEach((c, k) -> { if (k > config.maxRemindersPerCart()) v.add("S2 cap exceeded " + c + "=" + k); });

        // S3: never message holdout or guests.
        for (String k : p.provider.inbox.keySet()) {
            CartState cart = p.carts.get(cartOf(k));
            if ("control".equals(cart.arm) || cart.userId == null) v.add("S3 messaged ineligible cart " + k);
        }

        // S4: no reminder inside the inactivity window of the latest shopper event
        //     (only for carts whose events all arrived on time).
        for (Delivery d : p.provider.inbox.values()) {
            String cid = cartOf(d.key());
            if (delayed.contains(cid)) continue;
            long last = eventsByCart.get(cid).stream().mapToLong(CartEvent::occurredAt)
                    .filter(x -> x <= d.at()).max().orElseThrow();
            if (d.at() - last < config.inactivityWindowS()) v.add("S4 " + d.key() + " sent " + (d.at() - last) + "s after activity");
        }

        // L1: every scheduled reminder reached at least one terminal outcome.
        Set<String> scheduled = new HashSet<>();
        Set<String> resolved = new HashSet<>();
        for (Outcome o : p.outcomes.all()) {
            if (o.type() == Outcome.Type.REMINDER_SCHEDULED) scheduled.add(o.key());
            if (o.type().terminal()) resolved.add(o.key());
        }
        scheduled.stream().filter(k -> !resolved.contains(k)).limit(20).forEach(k -> v.add("L1 never resolved " + k));

        // L2: nothing left in the timer store.
        if (p.timers.pending() > 0) v.add("L2 " + p.timers.pending() + " timers still pending");
        return v;
    }

    private static String cartOf(String key) {
        return key.substring(0, key.indexOf(':'));
    }
}
