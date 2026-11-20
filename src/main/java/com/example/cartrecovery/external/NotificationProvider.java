package com.example.cartrecovery.external;

import com.example.cartrecovery.clock.Clock;
import com.example.cartrecovery.model.Trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fake email/push provider. No real sends: it records which triggers WOULD be delivered.
 * Honours a caller-supplied idempotency key, as real providers can: a repeated key is
 * acknowledged but not delivered twice. Faults are injected per call.
 */
public final class NotificationProvider {

    public enum Fault { TRANSIENT, PERMANENT }

    /** Decides the fault (or null) for the n-th call with a given key. */
    @FunctionalInterface
    public interface FaultPlan {
        Fault fault(String key, int callNumberForKey);
    }

    /** Timeouts, 5xx, throttling: worth retrying. */
    public static final class TransientSendException extends Exception {
        private static final long serialVersionUID = 1L;

        public TransientSendException(String m) { super(m); }
    }

    /** Invalid address, unsubscribed at provider, 4xx: retrying will not help. */
    public static final class PermanentSendException extends Exception {
        private static final long serialVersionUID = 1L;

        public PermanentSendException(String m) { super(m); }
    }

    public record Delivery(Trigger trigger, long at) {
        public String key() { return trigger.key(); }
    }

    private final Clock clock;
    private final FaultPlan faultPlan;
    private final Map<String, Integer> callsPerKey = new HashMap<>();
    /** Every attempt, including failures. */
    public final List<String> calls = new ArrayList<>();
    /** What the shopper actually receives, by idempotency key. */
    public final Map<String, Delivery> inbox = new HashMap<>();
    public int providerDedupHits;

    public NotificationProvider(Clock clock, FaultPlan faultPlan) {
        this.clock = clock;
        this.faultPlan = faultPlan;
    }

    public void send(Trigger trigger) throws TransientSendException, PermanentSendException {
        String key = trigger.key();
        int n = callsPerKey.merge(key, 1, Integer::sum);
        calls.add(key);
        Fault f = faultPlan == null ? null : faultPlan.fault(key, n);
        if (f == Fault.TRANSIENT) throw new TransientSendException("timeout sending " + key);
        if (f == Fault.PERMANENT) throw new PermanentSendException("rejected " + key);
        if (inbox.containsKey(key)) {
            providerDedupHits++;
            return;
        }
        inbox.put(key, new Delivery(trigger, clock.now()));
    }

    public long callCount(String key) {
        return calls.stream().filter(key::equals).count();
    }
}
