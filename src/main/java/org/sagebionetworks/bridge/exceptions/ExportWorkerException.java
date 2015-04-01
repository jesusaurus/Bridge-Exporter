package org.sagebionetworks.bridge.exceptions;

@SuppressWarnings("serial")
public class ExportWorkerException extends Exception {
    public ExportWorkerException() {
    }

    public ExportWorkerException(String message) {
        super(message);
    }

    public ExportWorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExportWorkerException(Throwable cause) {
        super(cause);
    }
}
