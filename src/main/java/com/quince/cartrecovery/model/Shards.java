package com.quince.cartrecovery.model;

public final class Shards {
    private Shards() {}

    public static int of(String cartId, int shards) {
        return Math.floorMod(cartId.hashCode(), shards);
    }
}
