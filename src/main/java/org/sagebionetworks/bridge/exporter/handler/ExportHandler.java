package org.sagebionetworks.bridge.exporter.handler;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;

/**
 * <p>
 * Generic export handler. This class encapsulates basic configuration serves as the parent class for both Synapse
 * export workers and iOS survey export workers.
 * </p>
 * <p>
 * Each handler represents a specific table or a task in a specific study. Multiple handlers will be instantiated
 * during the lifetime of Bridge-EX.
 * </p>
 */
public abstract class ExportHandler {
    private ExportWorkerManager manager;
    private String studyId;

    /** @see #setManager */
    protected final ExportWorkerManager getManager() {
        return manager;
    }

    /**
     * Export worker manager. Used by subclasses to access helpers and clients. Because export handlers are created on
     * the fly, we don't want the factory to get bogged down with setters, so we just use the manager and let Spring
     * take care of all the setters.
     */
    public final void setManager(ExportWorkerManager manager) {
        this.manager = manager;
    }

    /** @see #setStudyId */
    protected final String getStudyId() {
        return studyId;
    }

    /**
     * Study ID that this handler represents (or study ID that the schema of this handler lives in, if it represents a
     * schema). This is used as a key to a lot of things.
     */
    public final void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    /** Handles the task. Whatever it might be. */
    public abstract void handle(ExportSubtask subtask);
}
