package org.sagebionetworks.bridge.exporter.worker;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.sagebionetworks.client.exceptions.SynapseException;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.handler.ExportHandler;

/**
 * <p>
 * A runnable representing the asynchronous execution of an ExportSubtask. This exists separately of ExportSubtask so
 * as to separate state (ExportSubtask) from execution (ExportWorker), and separate from ExportHandlers so that we can
 * define a single ExportHandler to be re-used across all requests.
 * </p>
 * <p>
 * We use a callable instead of a runnable, because we want to be able to throw and catch checked exceptions.
 * Otherwise, we'd have to wrap these in a RuntimeException, which then get wrapped in an ExecutionException, and
 * things get messy.
 * </p>
 */
public class ExportWorker implements Callable<Void> {
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
    public Void call() throws BridgeExporterException, IOException, SchemaNotFoundException, SynapseException {
        handler.handle(subtask);

        // Callables have to have a return value. We don't have a return value, so return null.
        return null;
    }
}
