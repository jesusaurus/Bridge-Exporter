package org.sagebionetworks.bridge.exporter.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang.StringUtils;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.ExportHandler;
import org.sagebionetworks.bridge.exporter.handler.HealthDataExportHandler;
import org.sagebionetworks.bridge.exporter.handler.IosSurveyExportHandler;
import org.sagebionetworks.bridge.exporter.helper.ExportHelper;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;

// TODO doc
@Component
public class ExportWorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExportWorkerManager.class);

    // Public, so they can be accessed in handler unit tests.
    public static final String CONFIG_KEY_EXPORTER_DDB_PREFIX = "exporter.ddb.prefix";
    public static final String CONFIG_KEY_SYNAPSE_PRINCIPAL_ID = "synapse.principal.id";
    public static final String CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD =
            "worker.manager.progress.report.period";

    // CONFIG

    private String exporterDdbPrefix;
    private int progressReportPeriod;
    private long synapsePrincipalId;

    @Autowired
    public final void setConfig(Config config) {
        this.exporterDdbPrefix = config.get(CONFIG_KEY_EXPORTER_DDB_PREFIX);
        this.synapsePrincipalId = config.getInt(CONFIG_KEY_SYNAPSE_PRINCIPAL_ID);

        this.progressReportPeriod = config.getInt(CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD);
        if (progressReportPeriod == 0) {
            // Avoid mod by zero. Set to some reasonable hard-coded default.
            progressReportPeriod = 250;
        }
    }

    public final String getExporterDdbPrefixForTask(ExportTask task) {
        String override = task.getRequest().getExporterDdbPrefixOverride();
        if (StringUtils.isNotBlank(override)) {
            return override;
        }

        return exporterDdbPrefix;
    }

    public final long getSynapsePrincipalId() {
        return synapsePrincipalId;
    }

    // HELPER OBJECTS (CONFIGURED BY SPRING)

    private DynamoDB ddbClient;
    private DynamoHelper dynamoHelper;
    private ExportHelper exportHelper;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;

    public final DynamoDB getDdbClient() {
        return ddbClient;
    }

    @Autowired
    public final void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    public DynamoHelper getDynamoHelper() {
        return dynamoHelper;
    }

    @Autowired
    public void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    public final ExportHelper getExportHelper() {
        return exportHelper;
    }

    @Autowired
    public final void setExportHelper(ExportHelper exportHelper) {
        this.exportHelper = exportHelper;
    }

    public final FileHelper getFileHelper() {
        return fileHelper;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    public final SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    // STUDY INFO AND OVERRIDES

    // TODO re-doc
    /**
     * Returns team ID of the team allowed to read the data. This is derived from the study and from the study to
     * project mapping in the Bridge Exporter config.
     */
    public long getDataAccessTeamIdForStudy(String studyId) throws BridgeExporterException {
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);
        return studyInfo.getDataAccessTeamId();
    }

    // TODO re-doc
    /**
     * Returns the Synapse project ID. This is derived from the study and from the study to project mapping in the
     * Bridge Exporter config.
     */
    public String getSynapseProjectIdForStudyAndTask(String studyId, ExportTask task) throws BridgeExporterException {
        // check the override map
        Map<String, String> overrideMap = task.getRequest().getSynapseProjectOverrideMap();
        if (overrideMap != null && overrideMap.containsKey(studyId)) {
            return overrideMap.get(studyId);
        }

        // No override map. Fall back to study config (which is cached).
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);
        return studyInfo.getSynapseProjectId();
    }

    // TASK AND HANDLER MANAGEMENT

    private final Map<String, AppVersionExportHandler> appVersionHandlersByStudy = new HashMap<>();
    private ExecutorService executor;
    private final Map<UploadSchemaKey, HealthDataExportHandler> healthDataHandlersBySchema = new HashMap<>();
    private final Map<String, IosSurveyExportHandler> surveyHandlersByStudy = new HashMap<>();

    @Resource(name = "workerExecutorService")
    public final void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void addSubtaskForRecord(ExportTask task, Item record) throws BridgeExporterException, IOException,
            SchemaNotFoundException {
        UploadSchemaKey schemaKey = DynamoHelper.getSchemaKeyForRecord(record);
        String studyId = schemaKey.getStudyId();

        // Make subtask. Subtasks are immutable, so we can safely use the same one for each of the handlers.
        JsonNode recordDataNode = DefaultObjectMapper.INSTANCE.readTree(record.getString("data"));
        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(record).withParentTask(task)
                .withRecordData(recordDataNode).withSchemaKey(schemaKey).build();

        // Multiplex on schema.
        if ("ios-survey".equals(schemaKey.getSchemaId())) {
            // Special case: In the olden days, iOS surveys were processed by the Exporter instead of Bridge Server
            // Upload Validation. We don't do this anymore, but sometimes we want to re-export old uploads, so we still
            // need to handle this case.
            IosSurveyExportHandler surveyHandler = getSurveyHandlerForStudy(studyId);
            queueWorker(surveyHandler, task, subtask);
        } else {
            addHealthDataSubtask(task, studyId, schemaKey, subtask);
        }
    }

    // This is a separate method, because the IosSurveyExportHandler needs to call this directly.
    public void addHealthDataSubtask(ExportTask parentTask, String studyId, UploadSchemaKey schemaKey,
            ExportSubtask subtask) throws IOException, SchemaNotFoundException {
        AppVersionExportHandler appVersionHandler = getAppVersionHandlerForStudy(studyId);
        queueWorker(appVersionHandler, parentTask, subtask);

        HealthDataExportHandler healthDataHandler = getHealthDataHandlerForSchema(parentTask.getMetrics(), schemaKey);
        queueWorker(healthDataHandler, parentTask, subtask);
    }

    private void queueWorker(ExportHandler handler, ExportTask parentTask, ExportSubtask task) {
        ExportWorker worker = new ExportWorker(handler, task);
        Future<?> future = executor.submit(worker);
        parentTask.addOutstandingTask(future);
    }

    private AppVersionExportHandler getAppVersionHandlerForStudy(String studyId) {
        AppVersionExportHandler handler = appVersionHandlersByStudy.get(studyId);
        if (handler == null) {
            handler = new AppVersionExportHandler();
            handler.setManager(this);
            handler.setStudyId(studyId);
            appVersionHandlersByStudy.put(studyId, handler);
        }
        return handler;
    }

    private HealthDataExportHandler getHealthDataHandlerForSchema(Metrics metrics, UploadSchemaKey schemaKey)
            throws IOException, SchemaNotFoundException {
        HealthDataExportHandler handler = healthDataHandlersBySchema.get(schemaKey);
        if (handler == null) {
            handler = new HealthDataExportHandler();
            handler.setManager(this);
            handler.setStudyId(schemaKey.getStudyId());

            // set schema
            UploadSchema schema = dynamoHelper.getSchema(metrics, schemaKey);
            handler.setSchemaKey(schemaKey);
            handler.setSchema(schema);

            healthDataHandlersBySchema.put(schemaKey, handler);
        }
        return handler;
    }

    private IosSurveyExportHandler getSurveyHandlerForStudy(String studyId) {
        IosSurveyExportHandler handler = surveyHandlersByStudy.get(studyId);
        if (handler == null) {
            handler = new IosSurveyExportHandler();
            handler.setManager(this);
            handler.setStudyId(studyId);
            surveyHandlersByStudy.put(studyId, handler);
        }
        return handler;
    }

    public void endOfStream(ExportTask task) {
        BridgeExporterRequest request = task.getRequest();
        LOG.info("End of stream signaled for request with date=" + request.getDate() + ", tag=" + request.getTag());

        // Wait for all outstanding tasks to complete
        Stopwatch stopwatch = Stopwatch.createStarted();
        Queue<Future<?>> outstandingTaskQueue = task.getOutstandingTaskQueue();
        while (!outstandingTaskQueue.isEmpty()) {
            int numOutstanding = outstandingTaskQueue.size();
            if (numOutstanding % progressReportPeriod == 0) {
                LOG.info("Num outstanding tasks: " + numOutstanding + " after " + stopwatch.elapsed(TimeUnit.SECONDS) +
                        " seconds");
            }

            // ExportWorkers have no return value. If Future.get() returns normally, then the task is done.
            Future<?> oneFuture = outstandingTaskQueue.remove();
            try {
                oneFuture.get();
            } catch (ExecutionException | InterruptedException ex) {
                LOG.error("Error completing subtask: " + ex.getMessage(), ex);
            }
        }

        LOG.info("All subtasks done for request with date=" + request.getDate() + ", tag=" + request.getTag());

        // Tell each handler to upload their TSVs to Synapse.
        for (Map.Entry<UploadSchemaKey, HealthDataExportHandler> healthDataHandlerEntry
                : healthDataHandlersBySchema.entrySet()) {
            UploadSchemaKey schemaKey = healthDataHandlerEntry.getKey();
            HealthDataExportHandler handler = healthDataHandlerEntry.getValue();
            try {
                handler.uploadToSynapseForTask(task);
            } catch (BridgeExporterException | RuntimeException | SynapseException ex) {
                LOG.error("Error uploading health data to Synapse for schema=" + schemaKey + ": " + ex.getMessage(),
                        ex);
            }
        }
        for (Map.Entry<String, AppVersionExportHandler> appVersionHandlerEntry
                : appVersionHandlersByStudy.entrySet()) {
            String studyId = appVersionHandlerEntry.getKey();
            AppVersionExportHandler handler = appVersionHandlerEntry.getValue();
            try {
                handler.uploadToSynapseForTask(task);
            } catch (BridgeExporterException | RuntimeException | SynapseException ex) {
                LOG.error("Error uploading app version table to Synapse for study=" + studyId + ": " + ex.getMessage(),
                        ex);
            }
        }

        LOG.info("Done uploading to Synapse for request with date=" + request.getDate() + ", tag=" + request.getTag());
    }
}
