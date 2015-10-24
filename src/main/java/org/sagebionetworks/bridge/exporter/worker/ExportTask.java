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

// TODO
public class ExportTask {
    // TASK PARAMETERS

    private final LocalDate exporterDate;
    private final Metrics metrics;
    private final BridgeExporterRequest request;
    private final File tmpDir;

    private ExportTask(LocalDate exporterDate, Metrics metrics, BridgeExporterRequest request, File tmpDir) {
        this.exporterDate = exporterDate;
        this.metrics = metrics;
        this.request = request;
        this.tmpDir = tmpDir;
    }

    public LocalDate getExporterDate() {
        return exporterDate;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public BridgeExporterRequest getRequest() {
        return request;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public static class Builder {
        private LocalDate exporterDate;
        private Metrics metrics;
        private BridgeExporterRequest request;
        private File tmpDir;

        public Builder withExporterDate(LocalDate exporterDate) {
            this.exporterDate = exporterDate;
            return this;
        }

        public Builder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder withRequest(BridgeExporterRequest request) {
            this.request = request;
            return this;
        }

        public Builder withTmpDir(File tmpDir) {
            this.tmpDir = tmpDir;
            return this;
        }

        public ExportTask build() {
            // TODO validate
            return new ExportTask(exporterDate, metrics, request, tmpDir);
        }
    }

    // TASK STATE MANAGEMENT

    private final Map<String, TsvInfo> appVersionTsvInfoByStudy = new HashMap<>();
    private final Map<UploadSchemaKey, TsvInfo> healthDataTsvInfoBySchema = new HashMap<>();
    private final Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();

    public TsvInfo getAppVersionTsvInfoForStudy(String studyId) {
        return appVersionTsvInfoByStudy.get(studyId);
    }

    public void setAppVersionTsvInfoForStudy(String studyId, TsvInfo tsvInfo) {
        appVersionTsvInfoByStudy.put(studyId, tsvInfo);
    }

    public TsvInfo getHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey) {
        return healthDataTsvInfoBySchema.get(schemaKey);
    }

    public void setHealthDataTsvInfoForSchema(UploadSchemaKey schemaKey, TsvInfo tsvInfo) {
        healthDataTsvInfoBySchema.put(schemaKey, tsvInfo);
    }

    public Queue<Future<?>> getOutstandingTaskQueue() {
        return outstandingTaskQueue;
    }

    public void addOutstandingTask(Future<?> subtaskFuture) {
        outstandingTaskQueue.add(subtaskFuture);
    }
}
