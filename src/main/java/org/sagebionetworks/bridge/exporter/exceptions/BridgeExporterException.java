package org.sagebionetworks.bridge.exporter.exceptions;

/** General purpose "something went wrong with Bridge-EX" exception. */
@SuppressWarnings("serial")
public class BridgeExporterException extends Exception {
    public BridgeExporterException() {
    }

    public BridgeExporterException(String message) {
        super(message);
    }

    public BridgeExporterException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeExporterException(Throwable cause) {
        super(cause);
    }
}
