package org.sagebionetworks.bridge.exporter.worker;

import com.google.common.cache.CacheLoader;

import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO doc
public class HealthDataExportHandlerCacheLoader extends CacheLoader<UploadSchemaKey, HealthDataExportHandler> {
    private ExportWorkerManager exportWorkerManager;

    public final void setExportWorkerManager(ExportWorkerManager exportWorkerManager) {
        this.exportWorkerManager = exportWorkerManager;
    }

    @Override
    public HealthDataExportHandler load(UploadSchemaKey schemaKey) {
        HealthDataExportHandler healthDataHandler = new HealthDataExportHandler();
        healthDataHandler.setManager(exportWorkerManager);
        healthDataHandler.setName("healthDataWorker-" + schemaKey.toString());
        healthDataHandler.setSchemaKey(schemaKey);
        healthDataHandler.setStudyId(schemaKey.getStudyId());
        // TODO init
        return healthDataHandler;
    }
}
