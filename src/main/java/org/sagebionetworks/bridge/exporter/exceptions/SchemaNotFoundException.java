package org.sagebionetworks.bridge.exporter.exceptions;

/**
 * Thrown when we're unable to find the specific schema in DDB. This is used so we can track the schemas that are being
 * referenced but don't actually exist. (This is used mainly for legacy data. New data gets schema validated by the
 * Upload Validation API.)
 */
@SuppressWarnings("serial")
public class SchemaNotFoundException extends Exception {
    public SchemaNotFoundException() {
    }

    public SchemaNotFoundException(String message) {
        super(message);
    }

    public SchemaNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaNotFoundException(Throwable cause) {
        super(cause);
    }
}
