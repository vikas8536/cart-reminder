package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.ports.ArmAssigner;

/** Deterministic bucketing by shopper key and experiment salt. */
public final class HashArmAssigner implements ArmAssigner {
    private final String salt;
    private final int holdoutPercent;

    public HashArmAssigner(String salt, int holdoutPercent) {
        this.salt = salt;
        this.holdoutPercent = holdoutPercent;
    }

    @Override public Arm assign(String shopperKey) {
        int bucket = Math.floorMod((shopperKey + ":" + salt).hashCode(), 100);
        return bucket < holdoutPercent ? Arm.HOLDOUT : Arm.TREATMENT;
    }
}
