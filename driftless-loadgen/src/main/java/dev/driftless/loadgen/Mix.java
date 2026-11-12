package dev.driftless.loadgen;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/** Weighted choice of write operations, parsed from "insert=5,title=20,...". Weights are relative. */
record Mix(Map<Op, Integer> weights, int total) {

    static final String DEFAULT = "insert=5,title=20,body=20,touch=35,acl=10,delete=10";

    enum Op { INSERT, TITLE, BODY, TOUCH, ACL, DELETE }

    static Mix parse(String spec) {
        Map<Op, Integer> weights = new EnumMap<>(Op.class);
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=");
            Op op;
            try {
                op = Op.valueOf(kv[0].trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown operation in mix: " + kv[0] + "; use " + java.util.Arrays.toString(Op.values()));
            }
            int w = kv.length == 2 ? Integer.parseInt(kv[1].trim()) : -1;
            if (w < 0) {
                throw new IllegalArgumentException("weight for " + kv[0] + " must be a whole number >= 0");
            }
            weights.put(op, w);
        }
        int total = weights.values().stream().mapToInt(Integer::intValue).sum();
        if (total == 0) {
            throw new IllegalArgumentException("the mix needs at least one operation with a weight above 0");
        }
        return new Mix(weights, total);
    }

    Op draw(Random rnd) {
        int x = rnd.nextInt(total);
        for (Map.Entry<Op, Integer> e : weights.entrySet()) {
            x -= e.getValue();
            if (x < 0) {
                return e.getKey();
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
