package com.quince.cartrecovery.model;

/** The idempotency key "cartId:version:offsetIndex". Parsed from the right, so a cart id may contain ':'. */
public record LedgerKey(String cartId, long version, int offsetIndex) {

    @Override public String toString() {
        return cartId + ":" + version + ":" + offsetIndex;
    }

    public static LedgerKey parse(String key) {
        int last = key.lastIndexOf(':');
        int middle = last <= 0 ? -1 : key.lastIndexOf(':', last - 1);
        if (middle <= 0) throw new IllegalArgumentException("not a ledger key: " + key);
        try {
            return new LedgerKey(key.substring(0, middle),
                Long.parseLong(key.substring(middle + 1, last)),
                Integer.parseInt(key.substring(last + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a ledger key: " + key, e);
        }
    }
}
