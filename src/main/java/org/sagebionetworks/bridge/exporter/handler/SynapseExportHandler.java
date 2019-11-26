package org.sagebionetworks.bridge.exporter.handler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnChange;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterNonRetryableException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * This is a handler who's solely responsible for a single table in Synapse. This handler is assigned a stream of DDB
 * records to create a TSV, then uploads the TSV to the Synapse table. If the Synapse Table doesn't exist, this handler
 * will create it.
 */
public abstract class SynapseExportHandler extends ExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseExportHandler.class);

    private List<ColumnModel> commonColumnList;

    private List<ColumnDefinition> columnDefinition;

    private void initSynapseColumnDefinitionsAndColumnList() {
        this.columnDefinition = getManager().getColumnDefinitions();

        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnListBuilder.add(recordIdColumn);

        ColumnModel appVersionColumn = new ColumnModel();
        appVersionColumn.setName("appVersion");
        appVersionColumn.setColumnType(ColumnType.STRING);
        appVersionColumn.setMaximumSize(48L);
        columnListBuilder.add(appVersionColumn);

        ColumnModel phoneInfoColumn = new ColumnModel();
        phoneInfoColumn.setName("phoneInfo");
        phoneInfoColumn.setColumnType(ColumnType.STRING);
        phoneInfoColumn.setMaximumSize(48L);
        columnListBuilder.add(phoneInfoColumn);

        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName("uploadDate");
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);
        columnListBuilder.add(uploadDateColumn);

        final List<ColumnModel> tempList = BridgeExporterUtil.convertToColumnList(columnDefinition);
        columnListBuilder.addAll(tempList);

        this.commonColumnList = columnListBuilder.build();
    }

    /**
     * Given the record (contained in the subtask), serialize the results and write to a TSV. If a TSV hasn't been
     * created for this handler for the parent task, this will also initialize that TSV.
     */
    @Override
    public void handle(ExportSubtask subtask) throws BridgeExporterException, IOException, SchemaNotFoundException,
            SynapseException {
        String tableKey = getDdbTableKeyValue();
        ExportTask task = subtask.getParentTask();
        Metrics metrics = task.getMetrics();
        String recordId = subtask.getRecordId();

        try {
            // get TSV info (init if necessary)
            TsvInfo tsvInfo = initTsvForTask(task);
            tsvInfo.checkInitAndThrow();

            // Construct row value map. Merge row values from common columns and getTsvRowValueMap()
            Map<String, String> rowValueMap = new HashMap<>();
            rowValueMap.putAll(getCommonRowValueMap(subtask));
            rowValueMap.putAll(getTsvRowValueMap(subtask));

            // write to TSV
            tsvInfo.writeRow(rowValueMap);
            // add one record into tsv
            tsvInfo.addRecordId(recordId);
            metrics.incrementCounter(tableKey + ".lineCount");
        } catch (BridgeExporterException | IOException | RuntimeException | SchemaNotFoundException |
                SynapseException ex) {
            // Log metrics and rethrow.
            metrics.incrementCounter(tableKey + ".errorCount");
            LOG.error("Error processing record " + recordId + " for table " + tableKey + ": " + ex.getMessage(), ex);
            throw ex;
        }
    }

    // Gets the TSV for the task, initializing it if it hasn't been created yet. Also initializes the Synapse table if
    // it hasn't been created.
    private synchronized TsvInfo initTsvForTask(ExportTask task) {
        // check if the TSV is already saved in the task
        TsvInfo savedTsvInfo = getTsvInfoForTask(task);

        if (savedTsvInfo != null) {
            return savedTsvInfo;
        }

        TsvInfo tsvInfo;
        try {
            // get column name list
            List<String> columnNameList = getColumnNameList(task);

            // create TSV and writer
            FileHelper fileHelper = getManager().getFileHelper();

            // For the TSV filename, replace any characters that aren't alphanumeric, dash, or underscore. Since these
            // are temp files, we don't need to worry about readability, so don't worry about replacing these
            // characters with anything. Also append a short random string to (probabilistically) ensure uniqueness.
            String filename = getDdbTableKeyValue().replaceAll("[^A-Za-z0-9\\-_]", "") +
                    RandomStringUtils.randomAlphabetic(4) + ".tsv";

            File tsvFile = fileHelper.newFile(task.getTmpDir(), filename);
            Writer fileWriter = fileHelper.getWriter(tsvFile);

            // create TSV info
            tsvInfo = new TsvInfo(columnNameList, tsvFile, fileWriter);
        } catch (BridgeExporterException | FileNotFoundException | SchemaNotFoundException | SynapseException ex) {
            LOG.error("Error initializing TSV for table " + getDdbTableKeyValue() + ": " + ex.getMessage(), ex);
            tsvInfo = new TsvInfo(ex);
        }

        setTsvInfoForTask(task, tsvInfo);
        return tsvInfo;
    }

    // Gets the column name list from Synapse. If the Synapse table doesn't exist, this will create it. This is called
    // when initializing the TSV for a task.
    private List<String> getColumnNameList(ExportTask task) throws BridgeExporterException, SchemaNotFoundException,
            SynapseException {
        // Construct column definition list. Merge commonColumnList with getSynapseTableColumnList.
        initSynapseColumnDefinitionsAndColumnList();

        List<ColumnModel> columnDefList = new ArrayList<>();
        columnDefList.addAll(commonColumnList);
        columnDefList.addAll(getSynapseTableColumnList(task));

        // Create or update table if necessary.
        boolean isExisted = true;

        String synapseTableId = getManager().getSynapseTableIdFromDdb(task, getDdbTableName(), getDdbTableKeyName(),
                getDdbTableKeyValue());

        // check if the table in synapse currently
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();
        if (synapseTableId != null) {
            try {
                synapseHelper.getTableWithRetry(synapseTableId);
            } catch (SynapseNotFoundException e) {
                isExisted = false;
            }
        }

        if (synapseTableId == null || !isExisted) {
            createNewTable(task, columnDefList);
        } else {
            updateTableIfNeeded(synapseTableId, columnDefList);
        }

        // Extract column names from column models
        List<String> columnNameList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneColumnDef : columnDefList) {
            columnNameList.add(oneColumnDef.getName());
        }
        return columnNameList;
    }

    // Helper method to create the new Synapse table.
    private void createNewTable(ExportTask task, List<ColumnModel> columnDefList) throws BridgeExporterException,
            SynapseException {
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();

        // Delegate table creation to SynapseHelper.
        long dataAccessTeamId = manager.getDataAccessTeamIdForStudy(getStudyId());
        long principalId = manager.getSynapsePrincipalId();
        String projectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        String synapseTableId = synapseHelper.createTableWithColumnsAndAcls(columnDefList, dataAccessTeamId,
                principalId, projectId, getSynapseTableName());

        // write back to DDB table
        manager.setSynapseTableIdToDdb(task, getDdbTableName(), getDdbTableKeyName(), getDdbTableKeyValue(),
                synapseTableId);
    }

    // Helper method to detect when a schema changes and updates the Synapse table accordingly. Will reject schema
    // changes that delete or modify columns. Optimized so if no columns were inserted, it won't modify the table.
    private void updateTableIfNeeded(String synapseTableId, List<ColumnModel> columnDefList)
            throws BridgeExporterException, SynapseException {
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();

        // Get existing columns from table.
        List<ColumnModel> existingColumnList = synapseHelper.getColumnModelsForTableWithRetry(synapseTableId);

        // Compute the columns that were added, deleted, and kept.
        Map<String, ColumnModel> existingColumnsByName = Maps.uniqueIndex(existingColumnList, ColumnModel::getName);
        Map<String, ColumnModel> columnDefsByName = Maps.uniqueIndex(columnDefList, ColumnModel::getName);

        Set<String> addedColumnNameSet = Sets.difference(columnDefsByName.keySet(), existingColumnsByName.keySet());
        Set<String> deletedColumnNameSet = Sets.difference(existingColumnsByName.keySet(), columnDefsByName.keySet());
        Set<String> keptColumnNameSet = Sets.intersection(existingColumnsByName.keySet(), columnDefsByName.keySet());

        // Were columns deleted? If so, log an error and shortcut. (Don't modify the table.)
        boolean shouldThrow = false;
        if (!deletedColumnNameSet.isEmpty()) {
            LOG.error("Table " + getDdbTableKeyValue() + " has deleted columns: " +
                    BridgeExporterUtil.COMMA_SPACE_JOINER.join(deletedColumnNameSet));
            shouldThrow = true;
        }

        // Similarly, were any columns changed?
        Set<String> modifiedColumnNameSet = new HashSet<>();
        Set<String> incompatibleColumnNameSet = new HashSet<>();
        for (String oneKeptColumnName : keptColumnNameSet) {
            // Was this column modified? Check type and max length, since those are the only fields we care about. We
            // can't use .equals(), because newly generated columns don't have IDs.
            ColumnModel existingColumn = existingColumnsByName.get(oneKeptColumnName);
            ColumnModel columnDef = columnDefsByName.get(oneKeptColumnName);
            if (existingColumn.getColumnType() != columnDef.getColumnType() ||
                    !Objects.equals(existingColumn.getMaximumSize(), columnDef.getMaximumSize())) {
                modifiedColumnNameSet.add(oneKeptColumnName);
            } else {
                continue;
            }

            // This column was modified. Was the modification compatible?
            if (!SynapseHelper.isCompatibleColumn(existingColumn, columnDef)) {
                incompatibleColumnNameSet.add(oneKeptColumnName);
            }
        }
        if (!incompatibleColumnNameSet.isEmpty()) {
            LOG.error("Table " + getDdbTableKeyValue() + " has incompatible modified columns: " +
                    BridgeExporterUtil.COMMA_SPACE_JOINER.join(incompatibleColumnNameSet));
            shouldThrow = true;
        }

        if (shouldThrow) {
            throw new BridgeExporterNonRetryableException("Table has deleted and/or modified columns");
        }

        // Optimization: Were any columns added or modified?
        if (addedColumnNameSet.isEmpty() && modifiedColumnNameSet.isEmpty()) {
            return;
        }

        // Make sure the columns have been created / get column IDs.
        List<ColumnModel> createdColumnList = synapseHelper.createColumnModelsWithRetry(columnDefList);

        // Create list of column changes.
        List<ColumnChange> columnChangeList = new ArrayList<>();
        List<String> colIdList = new ArrayList<>();
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            ColumnModel existingColumn = existingColumnsByName.get(oneCreatedColumn.getName());
            String createdColumnId = oneCreatedColumn.getId();
            String existingColumnId = existingColumn != null ? existingColumn.getId() : null;

            // Only add column change if the column is different.
            if (!Objects.equals(createdColumnId, existingColumnId)) {
                ColumnChange columnChange = new ColumnChange();
                columnChange.setOldColumnId(existingColumnId);
                columnChange.setNewColumnId(createdColumnId);
                columnChangeList.add(columnChange);
            }

            // Want to specify order of columns.
            colIdList.add(createdColumnId);
        }

        // Create schema change request.
        TableSchemaChangeRequest schemaChangeRequest = new TableSchemaChangeRequest();
        schemaChangeRequest.setEntityId(synapseTableId);
        schemaChangeRequest.setChanges(columnChangeList);
        schemaChangeRequest.setOrderedColumnIds(colIdList);

        // Update table.
        synapseHelper.updateTableColumns(schemaChangeRequest, synapseTableId);
    }

    // Helper method to get row values that are common across all Synapse tables and handlers.
    private Map<String, String> getCommonRowValueMap(ExportSubtask subtask) {
        ExportTask task = subtask.getParentTask();
        Item record = subtask.getOriginalRecord();
        String recordId = subtask.getRecordId();

        // get phone and app info
        PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(record);
        String appVersion = phoneAppVersionInfo.getAppVersion();
        String phoneInfo = phoneAppVersionInfo.getPhoneInfo();

        // construct row
        Map<String, String> rowValueMap = new HashMap<>();
        rowValueMap.put("recordId", recordId);
        rowValueMap.put("appVersion", appVersion);
        rowValueMap.put("phoneInfo", phoneInfo);
        rowValueMap.put("uploadDate", task.getExporterDate().toString());

        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(rowValueMap,record, columnDefinition, recordId);

        return rowValueMap;
    }

    /**
     * This is called at the end of the record stream for a given export task. This will then upload the TSV to
     * Synapse.
     */
    public void uploadToSynapseForTask(ExportTask task) throws BridgeExporterException, IOException, SynapseException {
        ExportWorkerManager manager = getManager();
        TsvInfo tsvInfo = getTsvInfoForTask(task);
        if (tsvInfo == null) {
            // No TSV. This means we never wrote any records. Skip.
            return;
        }

        File tsvFile = tsvInfo.getFile();
        tsvInfo.flushAndCloseWriter();

        // filter on line count
        int lineCount = tsvInfo.getLineCount();
        if (lineCount > 0) {
            String projectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
            String synapseTableId = manager.getSynapseTableIdFromDdb(task, getDdbTableName(), getDdbTableKeyName(),
                    getDdbTableKeyValue());
            long linesProcessed = manager.getSynapseHelper().uploadTsvFileToTable(projectId, synapseTableId, tsvFile);
            if (linesProcessed != lineCount) {
                throw new BridgeExporterException("Wrong number of lines processed importing to table=" +
                        synapseTableId + ", expected=" + lineCount + ", actual=" + linesProcessed);
            }

            // call java sdk api to update records' exporter status
            postProcessTsv(tsvInfo);

            LOG.info("Done uploading to Synapse for table name=" + getDdbTableKeyValue() + ", id=" + synapseTableId);
        }

        // We've successfully processed the file. We can delete the file now.
        manager.getFileHelper().deleteFile(tsvFile);
    }

    /** Table name (excluding prefix) of the DDB table that holds Synapse table IDs. */
    protected abstract String getDdbTableName();

    /** Hash key name of the DDB table that holds Synapse table IDs. */
    protected abstract String getDdbTableKeyName();

    /**
     * Hash key value for the DDB table that holds the Synapse table IDs. Since this uniquely identifies the Synapse
     * table, and since Synapse table names need to be unique, this is also used as the Synapse table name.
     */
    protected abstract String getDdbTableKeyValue();

    /**
     * Unique name for the table to be created in Synapse. This name may be different from the table key in DDB, as
     * (for example) the table name doesn't need to include the study ID.
     */
    protected abstract String getSynapseTableName();

    /**
     * List of Synapse table column model objects, to be used to create both the column models and the Synapse table.
     * This excludes columns common to all Bridge tables defined in commonColumnList.
     */
    protected abstract List<ColumnModel> getSynapseTableColumnList(ExportTask task) throws SchemaNotFoundException;

    /** Get the TSV saved in the task for this handler. */
    protected abstract TsvInfo getTsvInfoForTask(ExportTask task);

    /** Save the TSV into the task for this handler. */
    protected abstract void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo);

    /** Creates a row values for a single row from the given export task. */
    protected abstract Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws BridgeExporterException,
            IOException, SchemaNotFoundException, SynapseException;


    /**
     * dummy method to implement by healthDataExportHandler to handle update record exporter status
     * @throws BridgeExporterException
     */
    protected void postProcessTsv(TsvInfo tsvInfo) throws BridgeExporterException {

    }
}
