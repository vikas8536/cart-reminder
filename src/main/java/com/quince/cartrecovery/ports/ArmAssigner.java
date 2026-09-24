package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Arm;

@FunctionalInterface
public interface ArmAssigner {
    Arm assign(String shopperKey);
}
