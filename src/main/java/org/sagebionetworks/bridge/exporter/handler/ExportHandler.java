package org.sagebionetworks.bridge.exporter.handler;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;

/**
 * Generic export handler. This class encapsulates basic configuration, helpers, and clients needed by export workers
 * and serves as the parent class for both Synapse export workers and iOS survey export workers.
 */
public abstract class ExportHandler {
    // Configured externally
    private ExportWorkerManager manager;
    private String studyId;

    /** Export worker manager. Configured externally. */
    protected final ExportWorkerManager getManager() {
        return manager;
    }

    public final void setManager(ExportWorkerManager manager) {
        this.manager = manager;
    }

    /** Study ID. Configured externally. */
    protected final String getStudyId() {
        return studyId;
    }

    public final void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    /** Handles the task. Whatever it might be. */
    public abstract void handle(ExportSubtask subtask);
}
