package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * An export task corresponds one-to-one with export requests. This class encapsulates the request as well as metadata
 * needed to process the request, such as metrics, temp dir, TSV info per table, and task queue.
 */
public class ExportTask {
    // TASK PARAMETERS

    private final LocalDate exporterDate;
    private final Metrics metrics;
    private final BridgeExporterRequest request;
    private final File tmpDir;

    /** Private constructor. To construct, use builder. */
    private ExportTask(LocalDate exporterDate, Metrics metrics, BridgeExporterRequest request, File tmpDir) {
        this.exporterDate = exporterDate;
        this.metrics = metrics;
        this.request = request;
        this.tmpDir = tmpDir;
    }

    /**
     * Calendar date that the Exporter ran. This is frequently different from the request date, since Exporter runs
     * generally export data from the day before.
     */
    public LocalDate getExporterDate() {
        return exporterDate;
    }

    /** Metrics collector for the task. */
    public Metrics getMetrics() {
        return metrics;
    }

    /** Original request correpsonding to the task. */
    public BridgeExporterRequest getRequest() {
        return request;
    }

    /**
     * Temporary directory in the local file system, used for temporary scratch space such as downloading attachments
     * or writing temporary TSV files.
     */
    public File getTmpDir() {
        return tmpDir;
    }

    /** Task builder. */
    public static class Builder {
        private LocalDate exporterDate;
        private Metrics metrics;
        private BridgeExporterRequest request;
        private File tmpDir;

        /** @see ExportTask#getExporterDate */
        public Builder withExporterDate(LocalDate exporterDate) {
            this.exporterDate = exporterDate;
            return this;
        }

        /** @see ExportTask#getMetrics */
        public Builder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        /** @see ExportTask#getRequest */
        public Builder withRequest(BridgeExporterRequest request) {
            this.request = request;
            return this;
        }

        /** @see ExportTask#getTmpDir */
        public Builder withTmpDir(File tmpDir) {
            this.tmpDir = tmpDir;
            return this;
        }

        /** Builds an ExportTask object and validates that all fields are non-null. */
        public ExportTask build() {
            // validate - all fields must be non-null
            if (exporterDate == null) {
                throw new IllegalStateException("exporterDate must be non-null");
            }

            if (metrics == null) {
                throw new IllegalStateException("metrics must be non-null");
            }

            if (request == null) {
                throw new IllegalStateException("request must be non-null");
            }

            if (tmpDir == null) {
                throw new IllegalStateException("tmpDir must be non-null");
            }

            return new ExportTask(exporterDate, metrics, request, tmpDir);
        }
    }

    // TASK STATE MANAGEMENT

    private final Map<String, TsvInfo> appVersionTsvInfoByStudy = new HashMap<>();
    private final Map<UploadSchemaKey, TsvInfo> healthDataTsvInfoBySchema = new HashMap<>();
    private final Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();

    /** Gets the appVersion table TSV info for the specified study. */
    public TsvInfo getAppVersionTsvInfoForStudy(String studyId) {
        return appVersionTsvInfoByStudy.get(studyId);
    }

    /** Sets the appVersion table TSV info for the specified study into the task. */
    public void setAppVersionTsvInfoForStudy(String studyId, TsvInfo tsvInfo) {
        appVersionTsvInfoByStudy.put(studyId, tsvInfo);
    }

    /** Gets the health data table TSV info for the specified schema. */
    public TsvInfo getHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey) {
        return healthDataTsvInfoBySchema.get(schemaKey);
    }

    /** Sets the health data table TSV info for the specified schema into the task. */
    public void setHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey, TsvInfo tsvInfo) {
        healthDataTsvInfoBySchema.put(schemaKey, tsvInfo);
    }

    /** Gets the task queue for the task. */
    public Queue<Future<?>> getOutstandingTaskQueue() {
        return outstandingTaskQueue;
    }

    /** Adds a subtask to the queue for the task. */
    public void addOutstandingTask(Future<?> subtaskFuture) {
        outstandingTaskQueue.add(subtaskFuture);
    }
}
