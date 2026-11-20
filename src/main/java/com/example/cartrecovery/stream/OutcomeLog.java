package com.example.cartrecovery.stream;

import com.example.cartrecovery.clock.Clock;
import com.example.cartrecovery.model.Outcome;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The {@code reminder-outcomes} stream: every decision, stamped with the clock. Production: a Kafka
 * topic feeding the warehouse for metrics and experiment readouts.
 */
public final class OutcomeLog {
    private final Clock clock;
    private final List<Outcome> outcomes = new ArrayList<>();

    public OutcomeLog(Clock clock) {
        this.clock = clock;
    }

    public void record(String cartId, Outcome.Type type, int idx, long version, String detail) {
        outcomes.add(new Outcome(clock.now(), cartId, type, idx, version, detail));
    }

    public void record(String cartId, Outcome.Type type, long version) {
        record(cartId, type, -1, version, "");
    }

    public List<Outcome> all() {
        return Collections.unmodifiableList(outcomes);
    }

    public List<Outcome> of(String cartId) {
        return outcomes.stream().filter(o -> o.cartId().equals(cartId)).toList();
    }

    public List<Outcome> of(String cartId, Outcome.Type type) {
        return outcomes.stream().filter(o -> o.cartId().equals(cartId) && o.type() == type).toList();
    }

    /** "idx@time" for every SENT outcome of the cart, in order. */
    public List<String> sentTimes(String cartId) {
        return of(cartId, Outcome.Type.SENT).stream().map(o -> o.idx() + "@" + o.t()).toList();
    }
}
