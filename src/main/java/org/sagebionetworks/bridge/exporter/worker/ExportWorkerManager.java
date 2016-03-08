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
import com.amazonaws.services.dynamodbv2.document.Table;
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
import org.sagebionetworks.bridge.exporter.synapse.SynapseStatusTableHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;

/**
 * This class manages export handlers and workers. This includes holding the config and helper objects that the
 * handlers need, routing requests to the right handler, and handling "end of stream" events.
 */
@Component
public class ExportWorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExportWorkerManager.class);

    // Public, so they can be accessed in handler unit tests.
    public static final String CONFIG_KEY_EXPORTER_DDB_PREFIX = "exporter.ddb.prefix";
    public static final String CONFIG_KEY_SYNAPSE_PRINCIPAL_ID = "synapse.principal.id";
    public static final String CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD =
            "worker.manager.progress.report.period";

    // package-scoped, to be available in tests
    static final String DDB_KEY_TABLE_ID = "tableId";
    static final String SCHEMA_IOS_SURVEY = "ios-survey";

    // CONFIG

    private String exporterDdbPrefix;
    private int progressReportPeriod;
    private long synapsePrincipalId;

    /** Bridge config. */
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

    /**
     * The prefix for DynamoDB tables for mapping Synapse tables. Examples: "prod-exporter-". This checks for overrides
     * in the task's request before falling back to the globally configured prefix.
     *
     * @param task
     *         export task, which may or may not have the DDB prefix override
     * @return resolved DDB prefix override
     */
    public String getExporterDdbPrefixForTask(ExportTask task) {
        String override = task.getRequest().getExporterDdbPrefixOverride();
        if (StringUtils.isNotBlank(override)) {
            return override;
        }

        return exporterDdbPrefix;
    }

    /** The Synapse principal ID (user ID) for the Bridge Exporter user. */
    public long getSynapsePrincipalId() {
        return synapsePrincipalId;
    }

    // DYNAMO DB HELPERS AND OVERRIDES

    /**
     * Gets the Synapse table ID, using the DDB Synapse table map. Returns null if the Synapse table doesn't exist (no
     * entry in the DDB table).
     *
     * @param task
     *         the export task, which contains context needed to get the Synapse table map
     * @param ddbTableName
     *         Dynamo DB table that contains the Synapse table map
     * @param ddbKeyName
     *         hash key name of the Dynamo DB table
     * @param ddbKeyValue
     *         value of the hash key of the Dynamo DB table (generally the Synapse table name)
     * @return Synapse table ID of the table, or null if it doesn't exist
     */
    public String getSynapseTableIdFromDdb(ExportTask task, String ddbTableName, String ddbKeyName,
            String ddbKeyValue) {
        Table synapseTableMap = getSynapseDdbTable(task, ddbTableName);
        Item tableMapItem = synapseTableMap.getItem(ddbKeyName, ddbKeyValue);
        if (tableMapItem != null) {
            return tableMapItem.getString(DDB_KEY_TABLE_ID);
        } else {
            return null;
        }
    }

    /**
     * Writes the Synapse table ID back to the DDB Synapse table map. This is called at the end of Synapse table
     * creation.
     *
     * @param task
     *         the export task, which contains context needed to get the Synapse table map
     * @param ddbTableName
     *         Dynamo DB table that contains the Synapse table map
     * @param ddbKeyName
     *         hash key name of the Dynamo DB table
     * @param ddbKeyValue
     *         value of the hash key of the Dynamo DB table (generally the Synapse table name)
     * @param synapseTableId
     *         Synapse table ID to write to Dynamo DB
     */
    public void setSynapseTableIdToDdb(ExportTask task, String ddbTableName, String ddbKeyName, String ddbKeyValue,
            String synapseTableId) {
        Table synapseTableMap = getSynapseDdbTable(task, ddbTableName);
        Item synapseTableNewItem = new Item();
        synapseTableNewItem.withString(ddbKeyName, ddbKeyValue);
        synapseTableNewItem.withString(DDB_KEY_TABLE_ID, synapseTableId);
        synapseTableMap.putItem(synapseTableNewItem);
    }

    // Helper method to get the DDB Synapse table map, called both to read and write the Synapse table ID to and from
    // DDB.
    private Table getSynapseDdbTable(ExportTask task, String ddbTableName) {
        String ddbPrefix = getExporterDdbPrefixForTask(task);
        return ddbClient.getTable(ddbPrefix + ddbTableName);
    }

    // HELPER OBJECTS (CONFIGURED BY SPRING)

    private DynamoDB ddbClient;
    private DynamoHelper dynamoHelper;
    private ExportHelper exportHelper;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;
    private SynapseStatusTableHelper synapseStatusTableHelper;

    /** DDB client, used to get the Synapse table mappings. */
    @Autowired
    public final void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** DynamoHelper, used to get schemas. */
    public DynamoHelper getDynamoHelper() {
        return dynamoHelper;
    }

    /** @see #getDynamoHelper */
    @Autowired
    public void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Export helper, used to handle attachments. */
    public final ExportHelper getExportHelper() {
        return exportHelper;
    }

    /** @see #getExportHelper */
    @Autowired
    public final void setExportHelper(ExportHelper exportHelper) {
        this.exportHelper = exportHelper;
    }

    /** File helper, used to create and write to TSVs in the file system. */
    public final FileHelper getFileHelper() {
        return fileHelper;
    }

    /** @see #getFileHelper */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Synapse Helper, used for creating Synapse tables and uploading data to Synapse tables. */
    public final SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    /** @see #getSynapseHelper */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Specialized helper to create and write to the Status table in Synapse. */
    @Autowired
    final void setSynapseStatusTableHelper(SynapseStatusTableHelper synapseStatusTableHelper) {
        this.synapseStatusTableHelper = synapseStatusTableHelper;
    }

    // STUDY INFO AND OVERRIDES

    /**
     * Convenience method for getting the access team ID for a given study. The access team ID is the team ID that is
     * allowed to read the exported data in Synapse. This calls through to DynamoDB and is present to maintain
     * backwards-compatibility and ease of migration.
     *
     * @param studyId
     *         study ID to get the access team ID for
     * @return access team ID
     */
    public long getDataAccessTeamIdForStudy(String studyId) {
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);
        return studyInfo.getDataAccessTeamId();
    }

    /**
     * The Synapse project ID to export to. This first checks the task's request for the project override, then falls
     * back to Dynamo DB.
     *
     * @param studyId
     *         study ID to get the Synapse project ID for
     * @param task
     *         export task, which may or may not have a project ID override
     * @return project ID to export to
     */
    public String getSynapseProjectIdForStudyAndTask(String studyId, ExportTask task) {
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

    /** Executor that runs our export workers. */
    @Resource(name = "workerExecutorService")
    public final void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Given the export task and one of the health data records in that task, this creates the export sub-tasks and
     * routes them to the appropriate export handlers. This returns immediately and queues up asynchronous workers to
     * handle those sub-tasks.
     *
     * @param task
     *         export task to be processed
     * @param record
     *         health data record within that export task to be processed
     * @throws IOException
     *         if reading the record data fails
     * @throws SchemaNotFoundException
     *         if the schema corresponding the record can't be found
     */
    public void addSubtaskForRecord(ExportTask task, Item record) throws IOException, SchemaNotFoundException {
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyForRecord(record);
        String studyId = schemaKey.getStudyId();

        // Book-keeping: We need to know what study IDs this task has seen.
        task.addStudyId(studyId);

        // Make subtask. Subtasks are immutable, so we can safely use the same one for each of the handlers.
        JsonNode recordDataNode = DefaultObjectMapper.INSTANCE.readTree(record.getString("data"));
        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(record).withParentTask(task)
                .withRecordData(recordDataNode).withSchemaKey(schemaKey).build();

        // Multiplex on schema.
        if (SCHEMA_IOS_SURVEY.equals(schemaKey.getSchemaId())) {
            // Special case: In the olden days, iOS surveys were processed by the Exporter instead of Bridge Server
            // Upload Validation. We don't do this anymore, but sometimes we want to re-export old uploads, so we still
            // need to handle this case.
            IosSurveyExportHandler surveyHandler = getSurveyHandlerForStudy(studyId);
            queueWorker(surveyHandler, task, subtask);
        } else {
            addHealthDataSubtask(task, studyId, schemaKey, subtask);
        }
    }

    /**
     * Add a health data sub-task. This is actually two sub-tasks, one for the app version table, one for the health
     * data table. This is a separate method, because the IosSurveyExportHandler needs to call this directly.
     *
     * @param parentTask
     *         parent export task that contains this health data sub-task
     * @param studyId
     *         study ID for this sub-task
     * @param schemaKey
     *         schema key for this sub-task
     * @param subtask
     *         object representing the sub-task
     * @throws IOException
     *         if getting the schema for the schema key fails
     * @throws SchemaNotFoundException
     *         if getting the schema for the schema key fails
     */
    public void addHealthDataSubtask(ExportTask parentTask, String studyId, UploadSchemaKey schemaKey,
            ExportSubtask subtask) throws IOException, SchemaNotFoundException {
        AppVersionExportHandler appVersionHandler = getAppVersionHandlerForStudy(studyId);
        queueWorker(appVersionHandler, parentTask, subtask);

        HealthDataExportHandler healthDataHandler = getHealthDataHandlerForSchema(parentTask.getMetrics(), schemaKey);
        queueWorker(healthDataHandler, parentTask, subtask);
    }

    /**
     * Adds the given task to the task queue.
     *
     * @param handler
     *         handler to queue up
     * @param parentTask
     *         parent export task, contains the task queue
     * @param subtask
     *         sub-task to queue up
     */
    private void queueWorker(ExportHandler handler, ExportTask parentTask, ExportSubtask subtask) {
        ExportWorker worker = new ExportWorker(handler, subtask);
        Future<?> future = executor.submit(worker);
        parentTask.addOutstandingTask(future);
    }

    /**
     * Gets the app version handler for the given study, with caching logic.
     *
     * @param studyId
     *         study ID for the app version handler
     * @return app version handler
     */
    private AppVersionExportHandler getAppVersionHandlerForStudy(String studyId) {
        AppVersionExportHandler handler = appVersionHandlersByStudy.get(studyId);
        if (handler == null) {
            handler = createAppVersionHandler(studyId);
            appVersionHandlersByStudy.put(studyId, handler);
        }
        return handler;
    }

    // Factory method for creating a new app version handler. This exists and is package-scoped to enable unit tests.
    AppVersionExportHandler createAppVersionHandler(String studyId) {
        AppVersionExportHandler handler = new AppVersionExportHandler();
        handler.setManager(this);
        handler.setStudyId(studyId);
        return handler;
    }

    /**
     * Gets the health data handler for the given schema key, with caching logic
     *
     * @param metrics
     *         metrics, used to track schema not found metrics
     * @param schemaKey
     *         schema for the health data handler
     * @return health data handler
     * @throws IOException
     *         if getting the schema fails
     * @throws SchemaNotFoundException
     *         if getting the schema fails
     */
    private HealthDataExportHandler getHealthDataHandlerForSchema(Metrics metrics, UploadSchemaKey schemaKey)
            throws IOException, SchemaNotFoundException {
        HealthDataExportHandler handler = healthDataHandlersBySchema.get(schemaKey);
        if (handler == null) {
            handler = createHealthDataHandler(metrics, schemaKey);
            healthDataHandlersBySchema.put(schemaKey, handler);
        }
        return handler;
    }

    // Factory method for creating a new health data handler. This exists and is package-scoped to enable unit tests.
    HealthDataExportHandler createHealthDataHandler(Metrics metrics, UploadSchemaKey schemaKey)
            throws IOException, SchemaNotFoundException {
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setManager(this);
        handler.setStudyId(schemaKey.getStudyId());

        // set schema
        UploadSchema schema = dynamoHelper.getSchema(metrics, schemaKey);
        handler.setSchema(schema);

        return handler;
    }

    /**
     * Gets the legacy survey handler for the given study, with caching logic.
     *
     * @param studyId
     *         study ID to get the handler for
     * @return legacy survey handler
     */
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

    /**
     * Signals the end of the record stream for the given export task. This waits for all of the outstanding tasks to
     * complete and signals the handlers to upload their TSVs to Synapse.
     *
     * @param task
     *         export task to be finished
     */
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
            } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
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
            } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
                LOG.error("Error uploading app version table to Synapse for study=" + studyId + ": " + ex.getMessage(),
                        ex);
            }
        }

        // Write status table. Status tables are individual for each study.
        for (String oneStudyId : task.getStudyIdSet()) {
            try {
                synapseStatusTableHelper.initTableAndWriteStatus(task, oneStudyId);
            } catch (BridgeExporterException | InterruptedException | SynapseException ex) {
                LOG.error("Error writing to status table for study=" + oneStudyId + ": " + ex.getMessage(), ex);
            }
        }

        LOG.info("Done uploading to Synapse for request with date=" + request.getDate() + ", tag=" + request.getTag());
    }
}
