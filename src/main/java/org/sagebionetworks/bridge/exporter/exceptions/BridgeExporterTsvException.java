package org.sagebionetworks.bridge.exporter.exceptions;

/**
 * A special kind of BridgeExporterException, which represents a TSV initialization failure. This exception itself
 * isn't thrown when the TSV is initialized, but is rather thrown later in the call chain when later calls attempt to
 * write to the TSV. This exception generally wraps the underlying cause so that later calls in the call chain can
 * handle the error appropriately.
 */
@SuppressWarnings("serial")
public class BridgeExporterTsvException extends BridgeExporterException {
    public BridgeExporterTsvException() {
    }

    public BridgeExporterTsvException(String message) {
        super(message);
    }

    public BridgeExporterTsvException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeExporterTsvException(Throwable cause) {
        super(cause);
    }
}
