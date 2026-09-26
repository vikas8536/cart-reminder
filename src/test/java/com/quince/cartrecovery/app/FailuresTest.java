package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

class FailuresTest {
    static DynamoDbException aws(int status, String code) {
        return (DynamoDbException) DynamoDbException.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
            .build();
    }

    @Test void validationErrorIsDeterministic() { assertTrue(Failures.isDeterministic(aws(400, "ValidationException"))); }
    @Test void throttlingIsTransient() { assertFalse(Failures.isDeterministic(aws(400, "ProvisionedThroughputExceededException"))); }
    @Test void serverErrorIsTransient() { assertFalse(Failures.isDeterministic(aws(500, "InternalServerError"))); }
    @Test void wrappedValidationErrorIsDeterministic() {
        assertTrue(Failures.isDeterministic(new CompletionException(aws(400, "ValidationException"))));
    }
    @Test void ioErrorIsTransient() { assertFalse(Failures.isDeterministic(new UncheckedIOException(new IOException("reset")))); }
}
