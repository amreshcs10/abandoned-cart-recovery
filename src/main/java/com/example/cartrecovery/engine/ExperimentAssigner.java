package com.example.cartrecovery.engine;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.model.Arm;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Sticky assignment with no lookup table: sha256(salt:unit) into buckets 0..99, split by arm
 * weight. The unit is the userId, or the cartId for guests.
 */
public final class ExperimentAssigner {
    private final String salt;
    private final List<Arm> arms;

    public ExperimentAssigner(Config config) {
        this.salt = config.experimentSalt();
        this.arms = config.arms();
    }

    public Arm assign(String unitId) {
        int bucket = (int) (Long.parseLong(sha256Hex(salt + ":" + unitId).substring(0, 8), 16) % 100);
        int acc = 0;
        for (Arm arm : arms) {
            acc += arm.weight();
            if (bucket < acc) return arm;
        }
        return arms.get(arms.size() - 1);
    }

    private static String sha256Hex(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
