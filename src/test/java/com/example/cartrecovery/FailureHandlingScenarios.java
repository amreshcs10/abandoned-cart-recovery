package com.example.cartrecovery;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.external.CartService;
import com.example.cartrecovery.external.NotificationProvider.Fault;
import com.example.cartrecovery.external.NotificationProvider.FaultPlan;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Outcome.Type;
import com.example.cartrecovery.model.Timer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.example.cartrecovery.Check.equal;
import static com.example.cartrecovery.Check.isTrue;
import static com.example.cartrecovery.TestSupport.CFG;
import static com.example.cartrecovery.TestSupport.T0;
import static com.example.cartrecovery.TestSupport.at;
import static com.example.cartrecovery.TestSupport.mins;
import static com.example.cartrecovery.TestSupport.outcomes;
import static com.example.cartrecovery.config.Config.DAY;
import static com.example.cartrecovery.config.Config.HOUR;

/** Retry, dead-letter, replay, expiry and consumer lag. */
public class FailureHandlingScenarios {

    @Scenario
    public void transientFailuresRetryWithBackoff() {
        FaultPlan plan = (key, n) -> key.endsWith(":r0") && n <= 2 ? Fault.TRANSIENT : null;
        Pipeline p = new Pipeline(CFG, T0, plan, null, Map.of());
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        long due = T0 + mins(30);
        equal(List.of(due, due + 30), p.of("c1", Type.RETRY_SCHEDULED).stream().map(Outcome::t).toList(),
                "backoff 30 s, then 60 s");
        equal(at(0, due + 30 + 60), p.sentTimes("c1").get(0));
    }

    @Scenario
    public void permanentFailureIsDeadLetteredThenReplayed() {
        AtomicBoolean broken = new AtomicBoolean(true);
        FaultPlan plan = (key, n) -> broken.get() && key.startsWith("c1:") ? Fault.PERMANENT : null;
        Pipeline p = new Pipeline(CFG, T0, plan, null, Map.of());
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.add("c2", T0, "u2"));
        p.runUntil(T0 + mins(31));
        equal(1, p.dlq.items().size());

        broken.set(false); // provider issue fixed
        p.runUntil(T0 + mins(40));
        equal(1, p.replayDlq());
        p.runUntil(T0 + mins(41));
        isTrue(p.sentTimes("c1").stream().anyMatch(s -> s.startsWith("0@")), "replayed reminder #0 sent");
    }

    @Scenario
    public void replayDoesNotRemindACartBoughtMeanwhile() {
        FaultPlan plan = (key, n) -> key.endsWith(":r0") ? Fault.PERMANENT : null;
        Pipeline p = new Pipeline(CFG, T0, plan, null, Map.of());
        CartService cs = new CartService();
        p.ingest(cs.add("c1", T0, "u1"));
        p.ingest(cs.checkout("c1", T0 + mins(35)));
        p.runUntil(T0 + mins(40));
        p.replayDlq();
        p.runUntil(T0 + 2 * DAY);

        equal(List.of(), p.sentTimes("c1"));
    }

    @Scenario
    public void exhaustedRetriesGoToTheDlq() {
        Pipeline p = new Pipeline(CFG, T0, (key, n) -> Fault.TRANSIENT, null, Map.of());
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        equal(3, p.dlq.items().size());
        equal(CFG.maxAttempts(), p.provider.callCount(Timer.reminderKey("c1", 1, 0)));
    }

    @Scenario
    public void outageLongerThanLatenessExpiresInsteadOfSendingStale() {
        Config cfg = Config.builder().arms(Arm.treatment("standard", 100)).maxAttempts(50).retryMaxS(HOUR).build();
        FaultPlan plan = (key, n) -> key.endsWith(":r0") ? Fault.TRANSIENT : null;
        Pipeline p = new Pipeline(cfg, T0, plan, null, Map.of());
        p.ingest(new CartService().add("c1", T0, "u1"));
        p.runUntil(T0 + 2 * DAY);

        equal(List.of("EXPIRED#0"), outcomes(p, "c1", Type.EXPIRED));
        equal(List.of(1, 2), p.of("c1", Type.SENT).stream().map(Outcome::idx).toList());
    }

    @Scenario
    public void consumerLagUsesEventTime() {
        Pipeline p = new Pipeline(CFG, T0);
        // The detector is down for 40 minutes; the add arrives late.
        p.ingest(new CartService().add("c1", T0, "u1"), T0 + mins(40));
        p.runUntil(T0 + 2 * DAY);

        equal(T0 + mins(40), p.of("c1", Type.ABANDONED).get(0).t(), "detected on arrival");
        equal(List.of(at(0, T0 + mins(40)), at(1, T0 + HOUR), at(2, T0 + DAY)), p.sentTimes("c1"));
    }
}
