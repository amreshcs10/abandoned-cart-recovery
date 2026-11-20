package com.example.cartrecovery;

import com.example.cartrecovery.clock.FakeClock;
import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.engine.AbandonmentDetector;
import com.example.cartrecovery.engine.ExperimentAssigner;
import com.example.cartrecovery.engine.TimerWorker;
import com.example.cartrecovery.external.NotificationProvider;
import com.example.cartrecovery.external.OrderService;
import com.example.cartrecovery.model.CartEvent;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Timer;
import com.example.cartrecovery.store.CartStateStore;
import com.example.cartrecovery.store.DeadLetterQueue;
import com.example.cartrecovery.store.SentLedger;
import com.example.cartrecovery.store.TimerStore;
import com.example.cartrecovery.stream.EventBus;
import com.example.cartrecovery.stream.OutcomeLog;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Wires the components together and drives them with a {@link FakeClock}.
 *
 *   CartService --> EventBus --> AbandonmentDetector --> CartStateStore (+ upsert DETECT timer)
 *                                                           |
 *   TimerStore --poll--> TimerWorker -----------------------+--> DETECT: abandoned, arm, reminder timers
 *                                                           +--> REMINDER: guards -> ledger -> provider
 *                                                                          retry / DLQ / expire
 */
public final class Pipeline {
    public final Config config;
    public final FakeClock clock;
    public final EventBus bus = new EventBus();
    public final CartStateStore carts = new CartStateStore();
    public final TimerStore timers;
    public final SentLedger ledger = new SentLedger();
    public final DeadLetterQueue dlq = new DeadLetterQueue();
    public final OrderService orders = new OrderService();
    public final NotificationProvider provider;
    public final OutcomeLog outcomes;

    private final AbandonmentDetector detector;
    private final TimerWorker worker;
    private long replays;

    public Pipeline(Config config, long start) {
        this(config, start, null, null, Map.of());
    }

    /**
     * @param faultPlan      provider faults to inject (null = none)
     * @param crashAfterSend (key, n) -> crash the worker after the n-th successful send of key
     * @param firstNames     userId -> first name, for the greeting
     */
    public Pipeline(Config config, long start, NotificationProvider.FaultPlan faultPlan,
                    BiPredicate<String, Integer> crashAfterSend, Map<String, String> firstNames) {
        this.config = config;
        this.clock = new FakeClock(start);
        this.timers = new TimerStore(config.leaseS());
        this.provider = new NotificationProvider(clock, faultPlan);
        this.outcomes = new OutcomeLog(clock);
        this.detector = new AbandonmentDetector(config, carts, timers, outcomes);
        this.worker = new TimerWorker(config, clock, carts, timers, ledger, dlq, provider, orders,
                new ExperimentAssigner(config), firstNames, outcomes, crashAfterSend);
    }

    /** The event reaches the detector when it happens. */
    public void ingest(CartEvent event) {
        bus.publish(event);
    }

    /** The event reaches the detector at {@code arriveAt}: models broker or consumer lag. */
    public void ingest(CartEvent event, long arriveAt) {
        bus.publish(event, arriveAt);
    }

    /**
     * Operator action after an incident: re-arm dead letters as fresh attempts. Every guard runs
     * again, so a cart bought in the meantime is not reminded.
     */
    public int replayDlq() {
        List<DeadLetterQueue.DeadLetter> items = dlq.drain();
        long now = clock.now();
        for (DeadLetterQueue.DeadLetter dl : items) {
            Timer t = dl.timer();
            timers.schedule(new Timer(t.key() + ":replay" + (++replays), Timer.Kind.REMINDER, t.cartId(), now,
                    t.version(), t.idx(), 1, now));
            outcomes.record(t.cartId(), Outcome.Type.DLQ_REPLAYED, t.idx(), t.version(), "");
        }
        return items.size();
    }

    /**
     * Advance the fake clock to {@code tEnd}, firing everything due on the way, in time order.
     * At equal times arrived events run before timers, so a checkout at 00:30:00 beats a reminder
     * due at 00:30:00.
     */
    public void runUntil(long tEnd) {
        for (long guard = 0; ; guard++) {
            if (guard > 50_000_000L) throw new IllegalStateException("runaway simulation");
            Long next = earliest(bus.nextArrival(), timers.nextTime());
            if (next == null || next > tEnd) break;
            clock.set(Math.max(next, clock.now()));
            for (CartEvent ev : bus.pollArrived(clock.now())) detector.handle(ev);
            worker.runDue();
        }
        clock.set(Math.max(tEnd, clock.now()));
    }

    private static Long earliest(Long a, Long b) {
        if (a == null) return b;
        if (b == null) return a;
        return Math.min(a, b);
    }

    // ---- shortcuts for tests and the verifier ------------------------------------------------

    public List<Outcome> of(String cartId) {
        return outcomes.of(cartId);
    }

    public List<Outcome> of(String cartId, Outcome.Type type) {
        return outcomes.of(cartId, type);
    }

    /** "idx@time" for every SENT outcome of the cart, in order. */
    public List<String> sentTimes(String cartId) {
        return outcomes.sentTimes(cartId);
    }
}
