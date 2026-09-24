package com.quince.cartrecovery.model;

public record CartItem(String sku, String name, int quantity, long priceCents) {}
