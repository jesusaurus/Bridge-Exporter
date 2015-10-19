package org.sagebionetworks.bridge.exporter.worker;

import com.google.common.cache.CacheLoader;

// TODO doc
public class IosSurveyExportHandlerCacheLoader extends CacheLoader<String, IosSurveyExportHandler> {
    private ExportWorkerManager exportWorkerManager;

    public final void setExportWorkerManager(ExportWorkerManager exportWorkerManager) {
        this.exportWorkerManager = exportWorkerManager;
    }

    @Override
    public IosSurveyExportHandler load(String studyId) {
        IosSurveyExportHandler surveyHandler = new IosSurveyExportHandler();
        surveyHandler.setManager(exportWorkerManager);
        surveyHandler.setName("surveyHandler-" + studyId);
        surveyHandler.setStudyId(studyId);
        // TODO init
        return surveyHandler;
    }
}
