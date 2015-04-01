package org.sagebionetworks.bridge.worker;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Generic export worker. This class encapsulates basic configuration, helpers, and clients needed by export workers
 * and serves as the parent class for both Synapse export workers and iOS survey export workers.
 */
public abstract class ExportWorker extends Thread {
    // Configured externally
    private ExportWorkerManager manager;
    private String studyId;

    // Internal state
    private final LinkedBlockingQueue<ExportTask> taskQueue = new LinkedBlockingQueue<>();

    /**
     * Returns team ID of the team allowed to read the data. This is derived from the study and from the study to
     * project mapping in the Bridge Exporter config.
     */
    public Long getDataAccessTeamId() {
        return manager.getBridgeExporterConfig().getDataAccessTeamIdsByStudy().get(studyId);
    }

    /** Export worker manager. Configured externally. */
    public ExportWorkerManager getManager() {
        return manager;
    }

    public void setManager(ExportWorkerManager manager) {
        this.manager = manager;
    }

    /**
     * Returns the Synapse project ID. This is derived from the study and from the study to project mapping in the
     * Bridge Exporter config.
     */
    public String getProjectId() {
        return manager.getBridgeExporterConfig().getProjectIdsByStudy().get(studyId);
    }

    /** Study ID. Configured externally. */
    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    /**
     * Takes (removes and returns) the next task from the task queue. The task queue is a blocking queue, so this will
     * block until there is a task to take (that is, until the queue is non-empty.
     */
    public ExportTask takeTask() throws InterruptedException {
        return taskQueue.take();
    }

    /**
     * Adds the given task to the task queue. The task queue is unbounded, so tasks are put into the queue immediately.
     */
    public void addTask(ExportTask task) {
        // task queue is unbounded, so add() always succeeds
        taskQueue.add(task);
    }

    /** Reports the metrics the worker has collected, if any. */
    protected abstract void reportMetrics();
}
