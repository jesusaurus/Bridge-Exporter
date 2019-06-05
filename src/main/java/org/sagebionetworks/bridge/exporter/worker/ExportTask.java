package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
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

    private final Map<UploadSchemaKey, TsvInfo> healthDataTsvInfoBySchema = new HashMap<>();
    private final Set<String> studyIdSet = new HashSet<>();
    private final Queue<ExportSubtaskFuture> subtaskFutureQueue = new LinkedList<>();
    private boolean success = false;
    private final Table<String, MetaTableType, TsvInfo> tsvInfoByStudyAndType = HashBasedTable.create();

    /** Gets the health data table TSV info for the specified schema. */
    public TsvInfo getHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey) {
        return healthDataTsvInfoBySchema.get(schemaKey);
    }

    /** Sets the health data table TSV info for the specified schema into the task. */
    public void setHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey, TsvInfo tsvInfo) {
        healthDataTsvInfoBySchema.put(schemaKey, tsvInfo);
    }

    /** Gets the queue for outstanding subtask executions. */
    public Queue<ExportSubtaskFuture> getSubtaskFutureQueue() {
        return subtaskFutureQueue;
    }

    /** Adds a subtask execution to the outstanding subtask queue. */
    public void addSubtaskFuture(ExportSubtaskFuture subtaskFuture) {
        subtaskFutureQueue.add(subtaskFuture);
    }

    /** Adds the study ID to the set of seen study IDs. */
    public void addStudyId(String studyId) {
        studyIdSet.add(studyId);
    }

    /** Gets the set of study IDs that were seen by this task. Used to do per-study post-processing. */
    public Set<String> getStudyIdSet() {
        return studyIdSet;
    }

    /** True if the task completed successfully. False if the task failed or otherwise did not complete. */
    public boolean isSuccess() {
        return success;
    }

    /** Sets the success status on the task. */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /** Gets the TSV info for the specified study and meta-table type. */
    public TsvInfo getTsvInfoForStudyAndType(String studyId, MetaTableType type) {
        return tsvInfoByStudyAndType.get(studyId, type);
    }

    /** Sets the TSV info for the specified study and meta-table type into the task. */
    public void setTsvInfoForStudyAndType(String studyId, MetaTableType type, TsvInfo tsvInfo) {
        tsvInfoByStudyAndType.put(studyId, type, tsvInfo);
    }
}
