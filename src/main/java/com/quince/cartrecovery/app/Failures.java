package com.quince.cartrecovery.app;

import java.util.Set;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/** Spec §6.3 failure classification. */
public final class Failures {
    private Failures() {}

    /**
     * Error codes that fail the same way on every retry because of the request itself. An allow-list: any other
     * 400 (ResourceNotFoundException for a deleted table, AccessDeniedException, bad credentials) is an
     * infrastructure fault and must be retried, never dead-lettered, or a purchase could be lost.
     */
    private static final Set<String> DETERMINISTIC_CODES = Set.of("ValidationException", "SerializationException");

    /** True only for AWS 400s whose error code is in the deterministic allow-list. */
    public static boolean isDeterministic(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof AwsServiceException e) {
                return e.statusCode() == 400 && DETERMINISTIC_CODES.contains(e.awsErrorDetails() == null ? null
                    : e.awsErrorDetails().errorCode());
            }
        }
        return false;
    }
}
