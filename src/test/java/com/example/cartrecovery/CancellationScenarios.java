package com.example.cartrecovery;

import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.model.CartStatus;
import com.example.cartrecovery.model.Outcome.Type;

import java.util.List;

import static com.example.cartrecovery.Check.equal;
import static com.example.cartrecovery.TestSupport.CFG;
import static com.example.cartrecovery.TestSupport.T0;
import static com.example.cartrecovery.TestSupport.at;
import static com.example.cartrecovery.TestSupport.mins;
import static com.example.cartrecovery.TestSupport.outcomes;
import static com.example.cartrecovery.config.Config.DAY;

/** Mid-flight cancellation: no reminder after purchase, clear, resume or merge. */
public class CancellationScenarios {

    @Scenario
    public void checkoutMidSequenceCancelsTheRest() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.checkout("c1", T0 + mins(45)));
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(at(0, T0 + mins(30))), p.sentTimes("c1"));
        equal(List.of("CANCELLED_STALE#1", "CANCELLED_STALE#2"), outcomes(p, "c1", Type.CANCELLED_STALE));
    }

    @Scenario
    public void checkoutAtTheSameInstantWins() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.checkout("c1", T0 + mins(30))); // exactly when detection is due
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(), p.sentTimes("c1"));
        equal(0, p.of("c1", Type.ABANDONED).size());
    }

    @Scenario
    public void laggingCheckoutEventIsCaughtByOrderService() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.runUntil(T0 + mins(50));                                   // reminder #0 sent at 30m
        p.orders.record("c1", T0 + mins(55));                        // order placed...
        p.ingest(cs.checkout("c1", T0 + mins(55)), T0 + mins(65));   // ...event 10 min late
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(at(0, T0 + mins(30))), p.sentTimes("c1"));
        equal(List.of("BLOCKED_BY_ORDER#1"), outcomes(p, "c1", Type.BLOCKED_BY_ORDER));
    }

    @Scenario
    public void clearAndRemovingLastItemBothStop() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.clear("c1", T0 + mins(31)));
        p.ingest(cs.add("c2", T0, "u2"));
        p.ingest(cs.remove("c2", T0 + mins(10), "sku-1"));
        p.runUntil(T0 + 2 * DAY);

        equal(1, p.sentTimes("c1").size());
        equal(List.of(), p.sentTimes("c2"));
        equal(CartStatus.CLEARED, p.carts.get("c2").status);
    }

    @Scenario
    public void resumeStopsTheSequenceAndRestartsTheClock() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.resume("c1", T0 + mins(40)));
        p.ingest(cs.checkout("c1", T0 + mins(50)));
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(at(0, T0 + mins(30))), p.sentTimes("c1"));
    }

    @Scenario
    public void closedCartIsNeverReopened() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.checkout("c1", T0 + mins(1)));
        p.ingest(cs.add("c1", T0 + mins(2), null, "late", 1, 10));
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(), p.sentTimes("c1"));
        equal(1, p.of("c1", Type.EVENT_IGNORED_CLOSED).size());
    }
}
