package com.example.cartrecovery;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.model.Arm;
import com.example.cartrecovery.model.Outcome;

import java.util.List;

import static com.example.cartrecovery.config.Config.MINUTE;

final class TestSupport {
    private TestSupport() {}

    static final long T0 = 1_000_000L; // arbitrary epoch seconds
    /** Everyone in treatment, so scenarios are not affected by the holdout hash. */
    static final Config CFG = Config.builder().arms(Arm.treatment("standard", 100)).build();

    /** "idx@time", matching {@link Pipeline#sentTimes}. */
    static String at(int idx, long t) {
        return idx + "@" + t;
    }

    static long mins(long m) {
        return m * MINUTE;
    }

    /** ["TYPE#idx", ...] for one outcome type of one cart. */
    static List<String> outcomes(Pipeline p, String cart, Outcome.Type type) {
        return p.of(cart, type).stream().map(o -> o.type() + "#" + o.idx()).toList();
    }
}
