package com.example.cartrecovery.verify;

import com.example.cartrecovery.Pipeline;
import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.external.NotificationProvider.Delivery;
import com.example.cartrecovery.external.NotificationProvider.Fault;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Outcome.Type;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.example.cartrecovery.config.Config.DAY;
import static com.example.cartrecovery.config.Config.MINUTE;

/**
 * Readable fake-clock walkthrough of five shoppers, then the 20,000-cart soak with its invariant
 * check. Exits non-zero if any invariant fails.
 */
public final class Verifier {
    private Verifier() {}

    private static final Set<Type> SHOW = EnumSet.of(Type.ACTIVITY, Type.ABANDONED, Type.HELD_OUT_CONTROL,
            Type.REMINDER_SCHEDULED, Type.SENT, Type.CANCELLED_STALE, Type.BLOCKED_BY_ORDER, Type.RETRY_SCHEDULED,
            Type.DEAD_LETTERED, Type.EXPIRED, Type.WORKER_CRASHED, Type.DUPLICATE_SKIPPED,
            Type.EVENT_IGNORED_STALE_OR_DUP, Type.SUPPRESSED_CAP, Type.SUPPRESSED_UNREACHABLE,
            Type.CLOSED_PURCHASED, Type.CLOSED_CLEARED, Type.CLOSED_MERGED);

    static String fmt(long t) {
        long d = t / 86400, rem = t % 86400;
        return String.format("d%d %02d:%02d:%02d", d, rem / 3600, (rem % 3600) / 60, rem % 60);
    }

    static void walkthrough() {
        Config config = Config.builder().arms(Arm.treatment("standard", 100)).build();
        Pipeline p = new Pipeline(config, 0,
                (key, n) -> key.equals("dave:v1:r0") && n <= 2 ? Fault.TRANSIENT : null,
                (key, n) -> key.equals("carol:v1:r1") && n == 1,
                Map.of("u-alice", "Alice", "u-bob", "Bob", "u-carol", "Carol", "u-dave", "Dave"));
        CartService cs = new CartService();

        Map<String, String> stories = new LinkedHashMap<>();
        stories.put("alice", "adds at 00:00, edits at 00:20, never returns -> 3 reminders from 00:20");
        stories.put("bob", "abandons, gets reminder #0 at 00:30, buys at 00:45 -> #1 and #2 cancelled");
        stories.put("carol", "worker crashes after sending #1 -> redelivered after lease, provider dedups");
        stories.put("dave", "provider times out twice on #0 -> retried with 30s, 60s backoff");
        stories.put("erin", "buys at 00:55 but CHECKOUT event arrives 15 min late -> order check blocks #1");

        p.ingest(cs.addNamed("alice", 0, "u-alice", "Trail shoes", 90));
        p.ingest(cs.update("alice", 20 * MINUTE, "sku-1", 2));
        p.ingest(cs.add("bob", 0, "u-bob"));
        p.ingest(cs.checkout("bob", 45 * MINUTE));
        p.ingest(cs.add("carol", 0, "u-carol"));
        p.ingest(cs.add("dave", 0, "u-dave"));
        p.ingest(cs.add("erin", 0, "u-erin"));
        p.orders.record("erin", 55 * MINUTE);
        p.ingest(cs.checkout("erin", 55 * MINUTE), 70 * MINUTE);
        p.runUntil(2 * DAY);

        stories.forEach((cid, story) -> {
            System.out.println("\n== " + cid + ": " + story);
            for (Outcome o : p.of(cid)) {
                if (!SHOW.contains(o.type())) continue;
                String idx = o.idx() >= 0 ? " #" + o.idx() : "";
                System.out.printf("   %s  %s%s  %s%n", fmt(o.t()), o.type(), idx, o.detail());
            }
        });
        System.out.println("\n== what the shopper would receive (no real send):");
        p.provider.inbox.values().stream()
                .sorted(Comparator.comparingLong(Delivery::at).thenComparing(Delivery::key))
                .forEach(d -> System.out.printf("   %s  %-16s %s %s%n", fmt(d.at()), d.key(),
                        d.trigger().greeting(), String.join(", ", d.trigger().items())));
    }

    public static void main(String[] args) {
        walkthrough();
        System.out.println("\n== soak: 20,000 carts, 10% holdout, 5% duplicate events, 1% events 10-40 min late,");
        System.out.println("   5% transient + 0.5% permanent send failures, 1% crashes between send and ack");
        Soak.Report rep = Soak.run(20_000, 42, Config.defaults());
        for (String k : List.of("ABANDONED", "HELD_OUT_CONTROL", "delivered", "CANCELLED_STALE", "BLOCKED_BY_ORDER",
                "RETRY_SCHEDULED", "WORKER_CRASHED", "provider_dedup_hits", "EVENT_IGNORED_STALE_OR_DUP",
                "SUPPRESSED_UNREACHABLE", "SUPPRESSED_CAP", "EXPIRED", "dlq_size")) {
            System.out.printf("   %-28s%8d%n", k, rep.count(k));
        }
        System.out.printf("   %d events processed in %.2fs wall (%,.0f events/s, single thread, incl. 3 simulated days)%n",
                rep.events(), rep.wallSeconds(), rep.events() / rep.wallSeconds());
        if (rep.ok()) {
            System.out.println("   invariants: PASS (no send after purchase, no duplicate send, no send to holdout/guest,");
            System.out.println("               no send inside the inactivity window, every reminder resolved, no stuck timers)");
            return;
        }
        System.out.println("   invariants: FAIL");
        rep.violations().stream().limit(20).forEach(s -> System.out.println("    " + s));
        System.exit(1);
    }
}
