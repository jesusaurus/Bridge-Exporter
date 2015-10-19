package org.sagebionetworks.bridge.exporter.worker;

import com.google.common.cache.CacheLoader;

// TODO doc
public class AppVersionExportHandlerCacheLoader extends CacheLoader<String, AppVersionExportHandler> {
    private ExportWorkerManager exportWorkerManager;

    public final void setExportWorkerManager(ExportWorkerManager exportWorkerManager) {
        this.exportWorkerManager = exportWorkerManager;
    }

    @Override
    public AppVersionExportHandler load(String studyId) {
        AppVersionExportHandler appVersionHandler = new AppVersionExportHandler();
        appVersionHandler.setManager(exportWorkerManager);
        appVersionHandler.setName("appVersionHandler-" + studyId);
        appVersionHandler.setStudyId(studyId);
        // TODO init
        return appVersionHandler;
    }
}
