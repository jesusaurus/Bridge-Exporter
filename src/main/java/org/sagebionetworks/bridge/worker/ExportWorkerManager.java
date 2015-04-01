package org.sagebionetworks.bridge.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;

public class ExportWorkerManager {
    // Configured externally
    private List<String> studyIdList;
    private UploadSchemaHelper schemaHelper;

    // Internal state
    private final Map<String, IosSurveyExportWorker> surveyWorkerMap = new HashMap<>();
    private final Map<String, AppVersionExportWorker> appVersionWorkerMap = new HashMap<>();
    private final Map<UploadSchemaKey, HealthDataExportWorker> healthDataWorkerMap = new HashMap<>();
    private final List<SynapseExportWorker> synapseWorkerList = new ArrayList<>();

    /** List of study IDs. Configured externally. */
    public List<String> getStudyIdList() {
        return studyIdList;
    }

    public void setStudyIdList(List<String> studyIdList) {
        this.studyIdList = studyIdList;
    }

    /** Schema helper. Configured externally. */
    public UploadSchemaHelper getSchemaHelper() {
        return schemaHelper;
    }

    public void setSchemaHelper(UploadSchemaHelper schemaHelper) {
        this.schemaHelper = schemaHelper;
    }

    public void init() {
        // TODO

        // init survey workers

        // init app version workers

        // init health data workers
    }
}
