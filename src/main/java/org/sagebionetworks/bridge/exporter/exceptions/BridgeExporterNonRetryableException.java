package org.sagebionetworks.bridge.exporter.exceptions;

/**
 * A special kind of BridgeExporterException. This represents a deterministic error that BridgeEX knows not to retry.
 */
@SuppressWarnings("serial")
public class BridgeExporterNonRetryableException extends BridgeExporterException {
    public BridgeExporterNonRetryableException() {
    }

    public BridgeExporterNonRetryableException(String message) {
        super(message);
    }

    public BridgeExporterNonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeExporterNonRetryableException(Throwable cause) {
        super(cause);
    }
}
