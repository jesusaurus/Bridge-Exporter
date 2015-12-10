package org.sagebionetworks.bridge.exporter.worker;

import org.sagebionetworks.bridge.exporter.handler.ExportHandler;

/**
 * A runnable representing the asynchronous execution of an ExportSubtask. This exists separately of ExportSubtask so
 * as to separate state (ExportSubtask) from execution (ExportWorker), and separate from ExportHandlers so that we can
 * define a single ExportHandler to be re-used across all requests.
 */
public class ExportWorker implements Runnable {
    private final ExportHandler handler;
    private final ExportSubtask subtask;

    /**
     * Creates an ExportWorker for the given handler and subtask.
     *
     * @param handler
     *         export handler to run
     * @param subtask
     *         export subtask to handle
     */
    public ExportWorker(ExportHandler handler, ExportSubtask subtask) {
        this.handler = handler;
        this.subtask = subtask;
    }

    /** Export handler to run. Package-scoped to be available to unit tests. */
    ExportHandler getHandler() {
        return handler;
    }

    /** Export subtask to handle. Package-scoped to be available to unit tests. */
    ExportSubtask getSubtask() {
        return subtask;
    }

    /**
     * Calls through to the given export handler with the given export subtask. This is called indirectly through the
     * ExecutorService.
     */
    @Override
    public void run() {
        handler.handle(subtask);
    }
}
