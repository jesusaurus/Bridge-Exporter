package org.sagebionetworks.bridge.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class ExportWorkerManager {
    private static final ExportTask END_OF_STREAM_TASK = new ExportTask(ExportTaskType.END_OF_STREAM, null, null,
            null);

    // Configured externally
    private BridgeExporterConfig bridgeExporterConfig;
    private DynamoDB ddbClient;
    private S3Helper s3Helper;
    private UploadSchemaHelper schemaHelper;
    private SynapseHelper synapseHelper;
    private String todaysDateString;

    // Internal state
    private final Set<String> schemasNotFound = new HashSet<>();
    private final Map<String, IosSurveyExportWorker> surveyWorkerMap = new HashMap<>();
    private final Map<String, AppVersionExportWorker> appVersionWorkerMap = new HashMap<>();
    private final Map<UploadSchemaKey, HealthDataExportWorker> healthDataWorkerMap = new HashMap<>();
    private final List<SynapseExportWorker> synapseWorkerList = new ArrayList<>();
    private final List<ExportWorker> allWorkerList = new ArrayList<>();

    /** Bridge exporter config. Configured externally. */
    public BridgeExporterConfig getBridgeExporterConfig() {
        return bridgeExporterConfig;
    }

    public void setBridgeExporterConfig(BridgeExporterConfig bridgeExporterConfig) {
        this.bridgeExporterConfig = bridgeExporterConfig;
    }

    /** DynamoDB client. Configured externally. */
    public DynamoDB getDdbClient() {
        return ddbClient;
    }

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** S3 helper. Configured externally. */
    public S3Helper getS3Helper() {
        return s3Helper;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Schema helper. Configured externally. */
    public UploadSchemaHelper getSchemaHelper() {
        return schemaHelper;
    }

    public void setSchemaHelper(UploadSchemaHelper schemaHelper) {
        this.schemaHelper = schemaHelper;
    }

    /** Synapse helper. Configured externally. */
    public SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    public void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Calendar date in YYYY-MM-DD format, representing when the data is uploaded to Synapse. (Because uploads can take
     * some time, this corresponds with when the upload started. Ideally, the uploads shouldn't take long enough to
     * cross over to the next day.
     */
    public String getTodaysDateString() {
        return todaysDateString;
    }

    public void setTodaysDateString(String todaysDateString) {
        this.todaysDateString = todaysDateString;
    }

    /** Returns a set of schemas that were queried but not found. */
    public Set<String> getSchemasNotFound() {
        return ImmutableSet.copyOf(schemasNotFound);
    }

    /** Initializes the worker threads for each study and each schema and kicks off the threads. */
    public void init() {
        // dedupe the study id list with a hash set
        Set<String> studyIdSet = new HashSet<>(bridgeExporterConfig.getStudyIdList());

        // per-study init
        for (String oneStudyId : studyIdSet) {
            // init survey workers
            IosSurveyExportWorker oneSurveyWorker = new IosSurveyExportWorker();
            oneSurveyWorker.setManager(this);
            oneSurveyWorker.setName("surveyWorker-" + oneStudyId);
            oneSurveyWorker.setStudyId(oneStudyId);
            surveyWorkerMap.put(oneStudyId, oneSurveyWorker);

            // init app version workers
            AppVersionExportWorker oneAppVersionWorker = new AppVersionExportWorker();
            oneAppVersionWorker.setManager(this);
            oneAppVersionWorker.setName("appVersionWorker-" + oneStudyId);
            oneAppVersionWorker.setStudyId(oneStudyId);
            appVersionWorkerMap.put(oneStudyId, oneAppVersionWorker);
        }

        // init health data workers for each schema
        Set<UploadSchemaKey> schemaKeySet = schemaHelper.getAllSchemaKeys();
        for (UploadSchemaKey oneSchemaKey : schemaKeySet) {
            HealthDataExportWorker oneHealthDataWorker = new HealthDataExportWorker();
            oneHealthDataWorker.setManager(this);
            oneHealthDataWorker.setName("healthDataWorker-" + oneSchemaKey.toString());
            oneHealthDataWorker.setSchemaKey(oneSchemaKey);
            oneHealthDataWorker.setStudyId(oneSchemaKey.getStudyId());
            healthDataWorkerMap.put(oneSchemaKey, oneHealthDataWorker);
        }

        synapseWorkerList.addAll(appVersionWorkerMap.values());
        synapseWorkerList.addAll(healthDataWorkerMap.values());

        allWorkerList.addAll(surveyWorkerMap.values());
        allWorkerList.addAll(appVersionWorkerMap.values());
        allWorkerList.addAll(healthDataWorkerMap.values());

        // init and start workers
        for (ExportWorker oneWorker : allWorkerList) {
            try {
                oneWorker.init();

                // only workers that are successfully inited will be started
                oneWorker.start();
            } catch (SchemaNotFoundException ex) {
                System.out.println("[ERROR] Schema not found for worker " + oneWorker.getName() + ": "
                        + ex.getMessage());
            } catch (ExportWorkerException ex) {
                System.out.println("[ERROR] Error initializing worker " + oneWorker.getName() + ": "
                        + ex.getMessage());
            } catch (RuntimeException ex) {
                System.out.println("[ERROR] RuntimeException initializing worker for table " + oneWorker.getName()
                        + ": " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }
    }

    /**
     * Queues up an iOS survey export task to the manager. This task will also transparently queue up the corresponding
     * PROCESS_IOS_SURVEY task to the right health data export worker and to the study's app version worker.
     */
    public void addIosSurveyExportTask(String studyId, ExportTask task) throws ExportWorkerException {
        IosSurveyExportWorker surveyWorker = surveyWorkerMap.get(studyId);
        if (surveyWorker == null) {
            throw new ExportWorkerException("No survey worker for study " + studyId);
        }
        surveyWorker.addTask(task);
    }

    /** Queues up an app version export task to the manager. */
    public void addAppVersionExportTask(String studyId, ExportTask task) throws ExportWorkerException {
        AppVersionExportWorker appVersionWorker = appVersionWorkerMap.get(studyId);
        if (appVersionWorker == null) {
            throw new ExportWorkerException("No app version worker for study " + studyId);
        }
        appVersionWorker.addTask(task);
    }

    /**
     * Queues up a health data export task to the manager. This is called by the Bridge Exporter as well as the iOS
     * survey export workers.
     */
    public void addHealthDataExportTask(UploadSchemaKey schemaKey, ExportTask task) throws SchemaNotFoundException {
        HealthDataExportWorker healthDataWorker = healthDataWorkerMap.get(schemaKey);
        if (healthDataWorker == null) {
            schemasNotFound.add(schemaKey.toString());
            throw new SchemaNotFoundException("Schema " + schemaKey.toString() + " not found");
        }
        healthDataWorker.addTask(task);
    }

    /**
     * Signals to the worker manager that the end of the record stream has been reached. This sends the corresponding
     * end of stream message to all worker threads (as an END_OF_STREAM task) and waits for all workers to finish.
     * This blocks until all workers are finished.
     */
    public void endOfStream() {
        System.out.println("[METRICS] End of stream signaled: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        // Since iOS survey workers can generate additional work for other workers, we need to handle iOS survey
        // workers first.
        // First loop signals all of the workers to finish.
        for (IosSurveyExportWorker oneSurveyWorker : surveyWorkerMap.values()) {
            oneSurveyWorker.addTask(END_OF_STREAM_TASK);
        }
        // Second loop waits for each thread to finish.
        for (IosSurveyExportWorker oneSurveyWorker : surveyWorkerMap.values()) {
            try {
                oneSurveyWorker.join();
            } catch (InterruptedException ex) {
                System.out.println("[ERROR] Interrupted waiting for survey worker " + oneSurveyWorker.getName()
                        + " to complete: " + ex.getMessage());
            }
        }

        System.out.println("[METRICS] Survey workers finished: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        // Now signal all the other workers (synapse workers) and then wait for them to finish.
        // First loop signals all of the workers to finish.
        for (SynapseExportWorker oneSynapseWorker : synapseWorkerList) {
            oneSynapseWorker.addTask(END_OF_STREAM_TASK);
        }
        // Second loop waits for each thread to finish.
        for (SynapseExportWorker oneSynapseWorker : synapseWorkerList) {
            try {
                oneSynapseWorker.join();
            } catch (InterruptedException ex) {
                System.out.println("[ERROR] Interrupted waiting for synapse worker " + oneSynapseWorker.getName()
                        + " to complete: " + ex.getMessage());
            }
        }

        // report metrics
        for (ExportWorker oneWorker : allWorkerList) {
            oneWorker.reportMetrics();
        }
    }
}
