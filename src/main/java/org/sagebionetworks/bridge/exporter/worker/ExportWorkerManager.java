package org.sagebionetworks.bridge.exporter.worker;

import java.util.concurrent.ExecutionException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.cache.LoadingCache;

import org.sagebionetworks.bridge.exporter.schema.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.study.StudyInfo;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO doc
public class ExportWorkerManager {
    private LoadingCache<String, AppVersionExportHandler> appVersionHandlerCache;
    private LoadingCache<UploadSchemaKey, HealthDataExportHandler> healthDataHandlerCache;
    private LoadingCache<String, StudyInfo> studyInfoCache;
    private LoadingCache<String, IosSurveyExportHandler> surveyHandlerCache;

    public final void setAppVersionHandlerCache(LoadingCache<String, AppVersionExportHandler> appVersionHandlerCache) {
        this.appVersionHandlerCache = appVersionHandlerCache;
    }

    public final void setHealthDataHandlerCache(
            LoadingCache<UploadSchemaKey, HealthDataExportHandler> healthDataHandlerCache) {
        this.healthDataHandlerCache = healthDataHandlerCache;
    }

    public final void setStudyInfoCache(LoadingCache<String, StudyInfo> studyInfoCache) {
        this.studyInfoCache = studyInfoCache;
    }

    public final void setSurveyHandlerCache(LoadingCache<String, IosSurveyExportHandler> surveyHandlerCache) {
        this.surveyHandlerCache = surveyHandlerCache;
    }

    public void addSubtaskForRecord(ExportTask task, Item record) throws BridgeExporterException {
        // Multiplex on schema.
        UploadSchemaKey schemaKey = UploadSchemaHelper.getSchemaKeyForRecord(record);
        if ("ios-survey".equals(schemaKey.getSchemaId())) {
            // Special case: In the olden days, iOS surveys were processed by the Exporter instead of Bridge Server
            // Upload Validation. We don't do this anymore, but sometimes we want to re-export old uploads, so we still
            // need to handle this case.
        } else {
            // TODO
        }
    }

    public void addSurveySubtask(ExportTask task, String studyId, Item record) throws BridgeExporterException {
        IosSurveyExportHandler surveyHandler;
        try {
            surveyHandler = surveyHandlerCache.get(studyId);
        } catch (ExecutionException ex) {
            throw new BridgeExporterException("No survey handler for study " + studyId + ": " + ex.getMessage(),
                    ex);
        }
    }
}
