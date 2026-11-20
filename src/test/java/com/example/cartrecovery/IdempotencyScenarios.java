package com.example.cartrecovery;

import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.model.CartEvent;
import com.example.cartrecovery.model.CartStatus;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Outcome.Type;
import com.example.cartrecovery.model.Timer;

import java.util.List;
import java.util.Map;

import static com.example.cartrecovery.Check.equal;
import static com.example.cartrecovery.TestSupport.CFG;
import static com.example.cartrecovery.TestSupport.T0;
import static com.example.cartrecovery.TestSupport.mins;
import static com.example.cartrecovery.TestSupport.outcomes;
import static com.example.cartrecovery.config.Config.DAY;

/** At-least-once everywhere, at most one reminder per slot reaching the shopper. */
public class IdempotencyScenarios {

    @Scenario
    public void duplicateAndOutOfOrderEventsAreIgnored() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        CartEvent e1 = cs.add("c1", T0, "u1");
        CartEvent e2 = cs.update("c1", T0 + mins(10), "sku-1", 2);
        CartEvent e3 = cs.checkout("c1", T0 + mins(20));
        p.ingest(e1);
        p.ingest(e1, T0 + 1);              // redelivered duplicate
        p.ingest(e3, T0 + mins(20));
        p.ingest(e2, T0 + mins(21));       // late, older event
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(), p.sentTimes("c1"));
        equal(CartStatus.PURCHASED, p.carts.get("c1").status);
        equal(2, p.of("c1", Type.EVENT_IGNORED_STALE_OR_DUP).size());
    }

    @Scenario
    public void crashAfterSendIsRedeliveredButShopperGetsOne() {
        Pipeline p = new Pipeline(CFG, T0, null, (key, n) -> key.endsWith(":r0") && n == 1, Map.of());
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        String k0 = Timer.reminderKey("c1", 1, 0);
        equal(2, p.provider.callCount(k0), "worker resent after crash");
        equal(1, p.provider.providerDedupHits, "provider dropped the repeat");
        equal(3, p.provider.inbox.size(), "one of each reminder delivered");
        Outcome sent0 = p.of("c1", Type.SENT).stream().filter(o -> o.idx() == 0).findFirst().orElseThrow();
        equal(T0 + mins(30) + CFG.leaseS(), sent0.t(), "redelivered after the lease");
    }

    @Scenario
    public void crashThenEditStillCountsTowardTheCap() {
        // Regression found by the soak: the provider accepted r0, the worker crashed, and the
        // shopper edited before redelivery. That message must count toward the cap.
        Pipeline p = new Pipeline(CFG, T0, null, (key, n) -> key.equals("c1:v1:r0"), Map.of());
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.add("c1", T0 + mins(30) + 10, null, "sku-2", 1, 10)); // inside the 60 s lease
        p.runUntil(T0 + 3 * DAY);

        equal(CFG.maxRemindersPerCart(), p.provider.inbox.size());
    }

    @Scenario
    public void duplicateTimerDeliveryIsSkippedByTheLedger() {
        Pipeline p = new Pipeline(CFG, T0);
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + mins(31));
        // Simulate the delay queue handing out an already-acked timer again.
        long now = p.clock.now();
        p.timers.schedule(new Timer("dup", Timer.Kind.REMINDER, "c1", now, 1, 0, 1, now));
        p.runUntil(T0 + mins(32));

        equal(List.of("DUPLICATE_SKIPPED#0"), outcomes(p, "c1", Type.DUPLICATE_SKIPPED));
        equal(1, p.provider.callCount(Timer.reminderKey("c1", 1, 0)));
    }
}
