package com.example.cartrecovery;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.engine.ExperimentAssigner;
import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.CartStatus;
import com.example.cartrecovery.model.Outcome.Type;
import com.example.cartrecovery.model.Timer;
import com.example.cartrecovery.model.Trigger;

import java.util.List;

import static com.example.cartrecovery.Check.equal;
import static com.example.cartrecovery.Check.isTrue;
import static com.example.cartrecovery.TestSupport.CFG;
import static com.example.cartrecovery.TestSupport.T0;
import static com.example.cartrecovery.TestSupport.at;
import static com.example.cartrecovery.TestSupport.mins;
import static com.example.cartrecovery.config.Config.DAY;
import static com.example.cartrecovery.config.Config.HOUR;

/** Holdout, experiment arms, guests and personalisation. */
public class SecondaryScenarios {

    @Scenario
    public void holdoutDetectsButNeverSends() {
        Pipeline p = new Pipeline(Config.builder().arms(Arm.control(100)).build(), T0);
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        equal(1, p.of("c1", Type.ABANDONED).size());
        equal(1, p.of("c1", Type.HELD_OUT_CONTROL).size());
        equal(List.of(), p.provider.calls);
    }

    @Scenario
    public void timingVariantArmUsesItsOwnOffsets() {
        Config cfg = Config.builder().arms(Arm.withOffsets("fast", 100, "urgent", mins(30), 2 * HOUR)).build();
        Pipeline p = new Pipeline(cfg, T0);
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(at(0, T0 + mins(30)), at(1, T0 + 2 * HOUR)), p.sentTimes("c1"));
    }

    @Scenario
    public void assignmentIsStickyAndRoughlyBalanced() {
        ExperimentAssigner assigner = new ExperimentAssigner(Config.defaults());
        equal(assigner.assign("u42").name(), assigner.assign("u42").name());
        int control = 0;
        for (int i = 0; i < 20_000; i++) if (assigner.assign("u" + i).name().equals("control")) control++;
        isTrue(control > 1700 && control < 2300, "control share " + control);
    }

    @Scenario
    public void guestCartIsTrackedButNotMessagedThenMerged() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        p.ingest(cs.add("guest-c", T0, null));
        p.runUntil(T0 + 2 * HOUR);
        equal(1, p.of("guest-c", Type.ABANDONED).size());
        isTrue(p.of("guest-c", Type.SENT).isEmpty(), "guest not messaged");

        // Shopper logs in: the Cart Service merges the guest cart into the user cart.
        p.ingest(cs.merged("guest-c", T0 + 3 * HOUR));
        p.ingest(cs.add("user-c", T0 + 3 * HOUR, "u9"));
        p.runUntil(T0 + 3 * DAY);

        equal(CartStatus.MERGED, p.carts.get("guest-c").status);
        equal(at(0, T0 + 3 * HOUR + mins(30)), p.sentTimes("user-c").get(0));
    }

    @Scenario
    public void personalisationTruncatesLongCarts() {
        Pipeline p = new Pipeline(CFG, T0);
        CartService cs = new CartService();
        for (int i = 0; i < 5; i++) p.ingest(cs.add("c1", T0, "u1", "s" + i, 1, 10.0 * (i + 1)));
        p.runUntil(T0 + mins(31));

        Trigger msg = p.provider.inbox.get(Timer.reminderKey("c1", 5, 0)).trigger();
        equal("Hi there,", msg.greeting());
        equal(List.of("1 x s4", "1 x s3", "1 x s2", "and 2 more"), msg.items());
    }
}
