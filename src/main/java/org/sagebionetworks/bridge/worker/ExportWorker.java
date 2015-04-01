package org.sagebionetworks.bridge.worker;

import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;

/**
 * Generic export worker. This class encapsulates basic configuration, helpers, and clients needed by export workers
 * and serves as the parent class for both Synapse export workers and iOS survey export workers.
 */
public abstract class ExportWorker implements Runnable {
    // Configured externally
    private DynamoDB ddbClient;
    private BridgeExporterConfig bridgeExporterConfig;
    private String studyId;

    // Internal state
    private final LinkedBlockingQueue<ExportTask> taskQueue = new LinkedBlockingQueue<>();

    /** DynamoDB client. Configured externally. */
    public DynamoDB getDdbClient() {
        return ddbClient;
    }

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** Bridge exporter config. Configured externally. */
    public BridgeExporterConfig getBridgeExporterConfig() {
        return bridgeExporterConfig;
    }

    public void setBridgeExporterConfig(BridgeExporterConfig bridgeExporterConfig) {
        this.bridgeExporterConfig = bridgeExporterConfig;
    }

    /**
     * Returns team ID of the team allowed to read the data. This is derived from the study and from the study to
     * project mapping in the Bridge Exporter config.
     */
    public Long getDataAccessTeamId() {
        return bridgeExporterConfig.getDataAccessTeamIdsByStudy().get(studyId);
    }

    /**
     * Returns the Synapse project ID. This is derived from the study and from the study to project mapping in the
     * Bridge Exporter config.
     */
    public String getProjectId() {
        return bridgeExporterConfig.getProjectIdsByStudy().get(studyId);
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
}
