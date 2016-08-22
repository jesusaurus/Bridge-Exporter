package org.sagebionetworks.bridge.exporter.exceptions;

/**
 * This is thrown if we need to completely abort the current request and try again later. This generally comes up when
 * Synapse is down for maintenance.
 */
@SuppressWarnings("serial")
public class RestartBridgeExporterException extends Exception {
    public RestartBridgeExporterException() {
    }

    public RestartBridgeExporterException(String message) {
        super(message);
    }

    public RestartBridgeExporterException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestartBridgeExporterException(Throwable cause) {
        super(cause);
    }
}
