package com.example.cartrecovery;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.Outcome.Type;
import com.example.cartrecovery.model.Timer;
import com.example.cartrecovery.model.Trigger;

import java.util.List;
import java.util.Map;

import static com.example.cartrecovery.Check.equal;
import static com.example.cartrecovery.Check.rejects;
import static com.example.cartrecovery.TestSupport.CFG;
import static com.example.cartrecovery.TestSupport.T0;
import static com.example.cartrecovery.TestSupport.at;
import static com.example.cartrecovery.TestSupport.mins;
import static com.example.cartrecovery.TestSupport.outcomes;
import static com.example.cartrecovery.config.Config.DAY;
import static com.example.cartrecovery.config.Config.HOUR;

/** When abandonment is detected and when reminders fire. */
public class DetectionScenarios {

    @Scenario
    public void happyPathFiresAtDefaultOffsets() {
        Pipeline p = new Pipeline(CFG, T0, null, null, Map.of("u1", "Asha"));
        p.ingest(new CartService().addNamed("c1", T0, "u1", "Running shoes", 80));

        p.runUntil(T0 + mins(30) - 1);
        equal(List.of(), p.sentTimes("c1"), "nothing before the window closes");

        p.runUntil(T0 + 2 * DAY);
        equal(List.of(at(0, T0 + mins(30)), at(1, T0 + HOUR), at(2, T0 + DAY)), p.sentTimes("c1"));
        equal(T0 + mins(30), p.of("c1", Type.ABANDONED).get(0).t());

        Trigger msg = p.provider.inbox.get(Timer.reminderKey("c1", 1, 0)).trigger();
        equal("Hi Asha,", msg.greeting());
        equal(List.of("1 x Running shoes"), msg.items());
    }

    @Scenario
    public void editResetsTheClock() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.update("c1", T0 + mins(20), "sku-1", 3));
        p.ingest(cs.add("c1", T0 + mins(45), null, "sku-2", 1, 10));
        p.runUntil(T0 + 2 * DAY);

        long last = T0 + mins(45);
        equal(List.of(at(0, last + mins(30)), at(1, last + HOUR), at(2, last + DAY)), p.sentTimes("c1"));
        equal(1, p.of("c1", Type.ABANDONED).size(), "earlier windows never completed");
    }

    @Scenario
    public void editAfterAbandonmentStartsNewEpisodeWithinTheCap() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.add("c1", T0 + mins(40), null, "sku-2", 1, 10)); // after reminder #0
        p.runUntil(T0 + 3 * DAY);

        // #0 of episode 1, then #0 and #1 of episode 2; the cap of 3 stops #2.
        equal(List.of(at(0, T0 + mins(30)), at(0, T0 + mins(70)), at(1, T0 + mins(100))), p.sentTimes("c1"));
        equal(List.of("SUPPRESSED_CAP#2"), outcomes(p, "c1", Type.SUPPRESSED_CAP));
    }

    @Scenario
    public void windowAndOffsetsAreConfigurable() {
        Config cfg = Config.builder().inactivityWindowS(mins(10)).reminderOffsetsS(mins(10), 2 * HOUR)
                .arms(Arm.treatment("standard", 100)).build();
        Pipeline p = new Pipeline(cfg, T0);
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + DAY);
        equal(List.of(at(0, T0 + mins(10)), at(1, T0 + 2 * HOUR)), p.sentTimes("c1"));
    }

    @Scenario
    public void invalidConfigIsRejected() {
        rejects(() -> Config.builder().inactivityWindowS(mins(30)).reminderOffsetsS(mins(10)).build());
        rejects(() -> Config.builder().reminderOffsetsS(HOUR, mins(30)).build());
        rejects(() -> Config.builder().arms(Arm.treatment("a", 50)).build());
    }
}
