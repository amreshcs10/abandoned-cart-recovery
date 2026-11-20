package com.example.cartrecovery.engine;

import com.example.cartrecovery.clock.Clock;
import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.external.NotificationProvider;
import com.example.cartrecovery.external.NotificationProvider.PermanentSendException;
import com.example.cartrecovery.external.NotificationProvider.TransientSendException;
import com.example.cartrecovery.external.OrderService;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.CartState;
import com.example.cartrecovery.model.CartStatus;
import com.example.cartrecovery.model.Item;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Timer;
import com.example.cartrecovery.model.Trigger;
import com.example.cartrecovery.store.CartStateStore;
import com.example.cartrecovery.store.DeadLetterQueue;
import com.example.cartrecovery.store.SentLedger;
import com.example.cartrecovery.store.TimerStore;
import com.example.cartrecovery.stream.OutcomeLog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Claims due timers and acts on them.
 * DETECT: mark the cart abandoned, assign an experiment arm, schedule its reminders.
 * REMINDER: run the guards, then send once using the ledger key as the idempotency key;
 * retry, dead-letter or expire on failure.
 */
public final class TimerWorker {

    /** Worker dies mid-task (after the provider call, before the ack). Test hook. */
    public static final class SimulatedCrash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SimulatedCrash(String key) { super(key); }
    }

    private final Config config;
    private final Clock clock;
    private final CartStateStore carts;
    private final TimerStore timers;
    private final SentLedger ledger;
    private final DeadLetterQueue dlq;
    private final NotificationProvider provider;
    private final OrderService orders;
    private final ExperimentAssigner assigner;
    private final Map<String, String> firstNames;
    private final OutcomeLog outcomes;
    private final BiPredicate<String, Integer> crashAfterSend;
    private final Map<String, Integer> crashCounts = new HashMap<>();

    public TimerWorker(Config config, Clock clock, CartStateStore carts, TimerStore timers, SentLedger ledger,
                       DeadLetterQueue dlq, NotificationProvider provider, OrderService orders,
                       ExperimentAssigner assigner, Map<String, String> firstNames, OutcomeLog outcomes,
                       BiPredicate<String, Integer> crashAfterSend) {
        this.config = config;
        this.clock = clock;
        this.carts = carts;
        this.timers = timers;
        this.ledger = ledger;
        this.dlq = dlq;
        this.provider = provider;
        this.orders = orders;
        this.assigner = assigner;
        this.firstNames = firstNames;
        this.outcomes = outcomes;
        this.crashAfterSend = crashAfterSend;
    }

    public void runDue() {
        for (Timer timer : timers.poll(clock.now())) {
            try {
                if (timer.kind() == Timer.Kind.DETECT) detect(timer);
                else remind(timer);
            } catch (SimulatedCrash crash) {
                // No ack: the lease runs out and the timer is redelivered.
                record(timer, Outcome.Type.WORKER_CRASHED, "");
                continue;
            }
            timers.ack(timer);
        }
    }

    // ---- detection -------------------------------------------------------------------------

    private void detect(Timer timer) {
        CartState cart = carts.get(timer.cartId());
        if (cart == null || cart.version != timer.version() || cart.status != CartStatus.ACTIVE) {
            outcomes.record(timer.cartId(), Outcome.Type.DETECT_STALE, timer.version());
            return;
        }
        cart.status = CartStatus.ABANDONED;
        cart.episodes++;
        String unit = cart.userId != null ? cart.userId : cart.cartId;
        Arm arm = assigner.assign(unit);
        cart.arm = arm.name();
        outcomes.record(cart.cartId, Outcome.Type.ABANDONED, -1, cart.version, "arm=" + arm.name() + " unit=" + unit);
        if (!arm.send()) {
            outcomes.record(cart.cartId, Outcome.Type.HELD_OUT_CONTROL, cart.version);
            return;
        }
        scheduleReminders(cart, config.offsetsFor(arm));
    }

    private void scheduleReminders(CartState cart, List<Long> offsets) {
        for (int idx = 0; idx < offsets.size(); idx++) {
            long due = cart.lastActivity + offsets.get(idx);
            timers.schedule(Timer.reminder(cart.cartId, cart.version, idx, due));
            outcomes.record(cart.cartId, Outcome.Type.REMINDER_SCHEDULED, idx, cart.version, "due_at=" + due);
        }
    }

    // ---- reminders -------------------------------------------------------------------------

    private void remind(Timer timer) {
        CartState cart = carts.get(timer.cartId());
        Outcome.Type blocked = guard(timer, cart);
        if (blocked != null) {
            record(timer, blocked, blocked == Outcome.Type.CANCELLED_STALE
                    ? (cart == null ? "missing" : cart.status.name())
                    : blocked == Outcome.Type.EXPIRED ? "late by " + (clock.now() - timer.dueAt()) + "s" : "");
            return;
        }
        send(timer, cart);
    }

    /** The checks before every send, in order. Returns the reason not to send, or null. */
    private Outcome.Type guard(Timer timer, CartState cart) {
        long now = clock.now();
        // 1. Version fence: any shopper event since abandonment bumped the version.
        if (cart == null || cart.version != timer.version() || cart.status != CartStatus.ABANDONED) {
            return Outcome.Type.CANCELLED_STALE;
        }
        // 2. Source of truth: an order exists even if its event has not arrived yet.
        if (orders.hasOrderSince(cart.cartId, cart.lastActivity, now)) return Outcome.Type.BLOCKED_BY_ORDER;
        // 3. Freshness: a 30-minute nudge delivered six hours late is noise.
        if (now - timer.dueAt() > config.maxLatenessS()) return Outcome.Type.EXPIRED;
        // 4. Eligibility and guardrails.
        if (!cart.contactable()) return Outcome.Type.SUPPRESSED_UNREACHABLE;
        if (ledger.get(timer.key()) == SentLedger.Status.SENT) return Outcome.Type.DUPLICATE_SKIPPED;
        if (!cart.counted.contains(timer.key()) && cart.remindersSent() >= config.maxRemindersPerCart()) {
            return Outcome.Type.SUPPRESSED_CAP;
        }
        return null;
    }

    /**
     * Claim (and count toward the cap), send with the ledger key as idempotency key, then mark
     * SENT. Only a send KNOWN to have failed releases the count.
     */
    private void send(Timer timer, CartState cart) {
        String key = timer.key();
        ledger.set(key, SentLedger.Status.PENDING);
        cart.counted.add(key);
        try {
            provider.send(trigger(cart, timer));
        } catch (TransientSendException e) {
            release(cart, key);
            retryOrDeadLetter(timer, e.getMessage());
            return;
        } catch (PermanentSendException e) {
            release(cart, key);
            dlq.put(timer, "permanent: " + e.getMessage(), clock.now());
            record(timer, Outcome.Type.DEAD_LETTERED, "permanent");
            return;
        }
        if (crashAfterSend != null) {
            int n = crashCounts.merge(key, 1, Integer::sum);
            if (crashAfterSend.test(key, n)) throw new SimulatedCrash(key); // provider got it; ledger PENDING
        }
        ledger.set(key, SentLedger.Status.SENT);
        record(timer, Outcome.Type.SENT, "attempt=" + timer.attempt());
    }

    private void release(CartState cart, String key) {
        ledger.set(key, SentLedger.Status.FAILED);
        cart.counted.remove(key);
    }

    private void retryOrDeadLetter(Timer timer, String reason) {
        long now = clock.now();
        if (timer.attempt() >= config.maxAttempts()) {
            dlq.put(timer, "retries exhausted: " + reason, now);
            record(timer, Outcome.Type.DEAD_LETTERED, "retries exhausted");
            return;
        }
        long backoff = Math.min(config.retryBaseS() * (1L << (timer.attempt() - 1)), config.retryMaxS());
        Timer next = timer.retryAt(now + backoff);
        timers.schedule(next);
        record(timer, Outcome.Type.RETRY_SCHEDULED, "attempt=" + next.attempt() + " in " + backoff + "s");
    }

    private void record(Timer t, Outcome.Type type, String detail) {
        outcomes.record(t.cartId(), type, t.idx(), t.version(), detail);
    }

    /** Personalisation: first-name greeting + top 3 items by line value + "and N more". */
    private Trigger trigger(CartState cart, Timer timer) {
        String first = cart.userId == null ? null : firstNames.get(cart.userId);
        List<Item> items = new ArrayList<>(cart.items.values());
        items.sort(Comparator.comparingDouble(Item::lineValue).reversed());
        List<String> shown = new ArrayList<>();
        for (Item i : items.subList(0, Math.min(3, items.size()))) shown.add(i.qty() + " x " + i.name());
        if (items.size() > 3) shown.add("and " + (items.size() - 3) + " more");
        return new Trigger(timer.key(), cart.cartId, cart.userId, cart.arm != null ? cart.arm : "default",
                timer.idx(), first != null ? "Hi " + first + "," : "Hi there,", List.copyOf(shown));
    }
}
