package org.sagebionetworks.bridge.exporter.exceptions;

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
