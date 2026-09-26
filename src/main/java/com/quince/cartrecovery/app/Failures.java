package com.quince.cartrecovery.app;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/** Spec §6.3 failure classification. */
public final class Failures {
    private Failures() {}

    /** True for errors that fail the same way on every retry: AWS 400s other than throttling and clock skew. */
    public static boolean isDeterministic(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof AwsServiceException e) {
                return e.statusCode() == 400 && !e.isThrottlingException() && !e.isClockSkewException();
            }
        }
        return false;
    }
}
