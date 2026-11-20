package com.example.cartrecovery;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.verify.Soak;

import static com.example.cartrecovery.Check.isTrue;

/** Randomised run with fault injection; every safety and liveness invariant must hold. */
public class SoakScenarios {

    @Scenario
    public void invariantsHoldUnderFaults() {
        for (long seed : new long[]{1, 2, 3}) {
            Soak.Report rep = Soak.run(5_000, seed, Config.defaults());
            isTrue(rep.ok(), String.join("\n", rep.violations().subList(0, Math.min(10, rep.violations().size()))));
            // The faults really happened, so the invariants were exercised.
            isTrue(rep.count("RETRY_SCHEDULED") > 0, "retries exercised");
            isTrue(rep.count("WORKER_CRASHED") > 0, "crashes exercised");
            isTrue(rep.count("EVENT_IGNORED_STALE_OR_DUP") > 0, "duplicates exercised");
            isTrue(rep.count("CANCELLED_STALE") > 0, "cancellations exercised");
        }
    }
}
