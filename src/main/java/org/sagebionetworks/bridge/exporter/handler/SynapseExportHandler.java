package org.sagebionetworks.bridge.exporter.handler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;

/**
 * This is a handler who's solely responsible for a single table in Synapse. This handler is assigned a stream of DDB
 * records to create a TSV, then uploads the TSV to the Synapse table. If the Synapse Table doesn't exist, this handler
 * will create it.
 */
public abstract class SynapseExportHandler extends ExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseExportHandler.class);

    // Package-scoped to be available to unit tests.
    static final List<ColumnModel> COMMON_COLUMN_LIST;
    static {
        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnListBuilder.add(recordIdColumn);

        ColumnModel healthCodeColumn = new ColumnModel();
        healthCodeColumn.setName("healthCode");
        healthCodeColumn.setColumnType(ColumnType.STRING);
        healthCodeColumn.setMaximumSize(36L);
        columnListBuilder.add(healthCodeColumn);

        ColumnModel externalIdColumn = new ColumnModel();
        externalIdColumn.setName("externalId");
        externalIdColumn.setColumnType(ColumnType.STRING);
        externalIdColumn.setMaximumSize(128L);
        columnListBuilder.add(externalIdColumn);

        ColumnModel dataGroupsColumn = new ColumnModel();
        dataGroupsColumn.setName("dataGroups");
        dataGroupsColumn.setColumnType(ColumnType.STRING);
        dataGroupsColumn.setMaximumSize(100L);
        columnListBuilder.add(dataGroupsColumn);

        // NOTE: ColumnType.DATE is actually a timestamp. There is no calendar date type.
        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName("uploadDate");
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);
        columnListBuilder.add(uploadDateColumn);

        ColumnModel createdOnColumn = new ColumnModel();
        createdOnColumn.setName("createdOn");
        createdOnColumn.setColumnType(ColumnType.DATE);
        columnListBuilder.add(createdOnColumn);

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

        COMMON_COLUMN_LIST = columnListBuilder.build();
    }

    private static final Joiner DATA_GROUP_JOINER = Joiner.on(',').useForNull("");

    /**
     * Given the record (contained in the subtask), serialize the results and write to a TSV. If a TSV hasn't been
     * created for this handler for the parent task, this will also initialize that TSV.
     */
    @Override
    public void handle(ExportSubtask subtask) {
        String tableKey = getDdbTableKeyValue();
        ExportTask task = subtask.getParentTask();
        Metrics metrics = task.getMetrics();
        String recordId = subtask.getOriginalRecord().getString("id");

        try {
            // get TSV info (init if necessary)
            TsvInfo tsvInfo = initTsvForTask(task);

            // Construct row value map. Merge row values from common columns and getTsvRowValueMap()
            Map<String, String> rowValueMap = new HashMap<>();
            rowValueMap.putAll(getCommonRowValueMap(subtask));
            rowValueMap.putAll(getTsvRowValueMap(subtask));

            // write to TSV
            tsvInfo.writeRow(rowValueMap);
            metrics.incrementCounter(tableKey + ".lineCount");
        } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
            metrics.incrementCounter(tableKey + ".errorCount");
            LOG.error("Error processing record " + recordId + " for table " + tableKey + ": " + ex.getMessage(), ex);
        }
    }

    // Gets the TSV for the task, initializing it if it hasn't been created yet. Also initializes the Synapse table if
    // it hasn't been created.
    private synchronized TsvInfo initTsvForTask(ExportTask task) throws BridgeExporterException {
        // check if the TSV is already saved in the task
        TsvInfo savedTsvInfo = getTsvInfoForTask(task);
        if (savedTsvInfo != null) {
            return savedTsvInfo;
        }

        try {
            // get column name list
            List<String> columnNameList = getColumnNameList(task);

            // create TSV and writer
            FileHelper fileHelper = getManager().getFileHelper();
            File tsvFile = fileHelper.newFile(task.getTmpDir(), getDdbTableKeyValue() + ".tsv");
            PrintWriter tsvWriter = new PrintWriter(fileHelper.getWriter(tsvFile));

            // create TSV info
            TsvInfo tsvInfo = new TsvInfo(columnNameList, tsvFile, tsvWriter);
            setTsvInfoForTask(task, tsvInfo);
            return tsvInfo;
        } catch (FileNotFoundException | SynapseException ex) {
            throw new BridgeExporterException("Error initializing TSV: " + ex.getMessage(), ex);
        }
    }

    // Gets the column name list from Synapse. If the Synapse table doesn't exist, this will create it. This is called
    // when initializing the TSV for a task.
    private List<String> getColumnNameList(ExportTask task) throws BridgeExporterException, SynapseException {
        // Construct column definition list. Merge COMMON_COLUMN_LIST with getSynapseTableColumnList.
        List<ColumnModel> columnDefList = new ArrayList<>();
        columnDefList.addAll(COMMON_COLUMN_LIST);
        columnDefList.addAll(getSynapseTableColumnList());

        // Create or update table if necessary. Get the column list from the create() or update() call to make sure we
        // have a column list that matches the Synapse table.
        List<ColumnModel> serverColumnList;
        String synapseTableId = getManager().getSynapseTableIdFromDdb(task, getDdbTableName(), getDdbTableKeyName(),
                getDdbTableKeyValue());
        if (synapseTableId == null) {
            serverColumnList = createTableAndGetColumnList(task, columnDefList);
        } else {
            serverColumnList = updateTableAndGetColumnList(synapseTableId, columnDefList);
        }

        // Extract column names from column models
        List<String> columnNameList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneServerColumn : serverColumnList) {
            columnNameList.add(oneServerColumn.getName());
        }
        return columnNameList;
    }

    // Helper method to create the new Synapse table. Returns the list of columns, with column IDs.
    private List<ColumnModel> createTableAndGetColumnList(ExportTask task, List<ColumnModel> columnDefList)
            throws BridgeExporterException, SynapseException {
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();

        // Delegate table creation to SynapseHelper.
        long dataAccessTeamId = manager.getDataAccessTeamIdForStudy(getStudyId());
        long principalId = manager.getSynapsePrincipalId();
        String projectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        String tableName = getDdbTableKeyValue();
        String synapseTableId = synapseHelper.createTableWithColumnsAndAcls(columnDefList, dataAccessTeamId,
                principalId, projectId, tableName);

        // write back to DDB table
        manager.setSynapseTableIdToDdb(task, getDdbTableName(), getDdbTableKeyName(), getDdbTableKeyValue(),
                synapseTableId);

        // Get the column list.
        return synapseHelper.getColumnModelsForTableWithRetry(synapseTableId);
    }

    // Helper method to detect when a schema changes and updates the Synapse table accordingly. Will reject schema
    // changes that delete or modify columns. Optimized so if no columns were inserted, it won't modify the table.
    // Returns the list of columns that matches what's on Synapse.
    private List<ColumnModel> updateTableAndGetColumnList(String synapseTableId, List<ColumnModel> columnDefList)
            throws SynapseException {
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();

        // Get existing columns from table.
        List<ColumnModel> existingColumnList = synapseHelper.getColumnModelsForTableWithRetry(synapseTableId);

        // Compute the columns that were added, deleted, and kept. Use tree maps so logging will show a stable message.
        Map<String, ColumnModel> existingColumnsByName = new TreeMap<>();
        for (ColumnModel oneExistingColumn : existingColumnList) {
            existingColumnsByName.put(oneExistingColumn.getName(), oneExistingColumn);
        }

        Map<String, ColumnModel> columnDefsByName = new TreeMap<>();
        for (ColumnModel oneColumnDef : columnDefList) {
            columnDefsByName.put(oneColumnDef.getName(), oneColumnDef);
        }

        Set<String> addedColumnNameSet = Sets.difference(columnDefsByName.keySet(), existingColumnsByName.keySet());
        Set<String> deletedColumnNameSet = Sets.difference(existingColumnsByName.keySet(), columnDefsByName.keySet());
        Set<String> keptColumnNameSet = Sets.intersection(existingColumnsByName.keySet(), columnDefsByName.keySet());

        // Were columns deleted? If so, log an error and shortcut. (Don't modify the table.)
        boolean shouldUpdateTable = true;
        if (!deletedColumnNameSet.isEmpty()) {
            LOG.error("Table " + getDdbTableKeyValue() + " has deleted columns: " +
                    BridgeExporterUtil.COMMA_SPACE_JOINER.join(deletedColumnNameSet));
            shouldUpdateTable = false;
        }

        // Similarly, were any columns changed?
        Set<String> modifiedColumnNameSet = new TreeSet<>();
        for (String oneKeptColumnName : keptColumnNameSet) {
            // Validate that column type and max size are the same. We can't use .equals() because ID is definitely
            // different.
            ColumnModel existingColumn = existingColumnsByName.get(oneKeptColumnName);
            ColumnModel columnDef = columnDefsByName.get(oneKeptColumnName);
            if (existingColumn.getColumnType() != columnDef.getColumnType() ||
                    !Objects.equals(existingColumn.getMaximumSize(), columnDef.getMaximumSize())) {
                modifiedColumnNameSet.add(oneKeptColumnName);
            }
        }
        if (!modifiedColumnNameSet.isEmpty()) {
            LOG.error("Table " + getDdbTableKeyValue() + " has modified columns: " +
                    BridgeExporterUtil.COMMA_SPACE_JOINER.join(modifiedColumnNameSet));
            shouldUpdateTable = false;
        }

        // Optimization: Were any columns added?
        if (addedColumnNameSet.isEmpty()) {
            shouldUpdateTable = false;
        }

        if (!shouldUpdateTable) {
            return existingColumnList;
        }

        // Make sure the columns have been created / get column IDs.
        List<ColumnModel> createdColumnList = synapseHelper.createColumnModelsWithRetry(columnDefList);

        // Update table.
        List<String> colIdList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            colIdList.add(oneCreatedColumn.getId());
        }

        TableEntity table = synapseHelper.getTableWithRetry(synapseTableId);
        table.setColumnIds(colIdList);
        synapseHelper.updateTableWithRetry(table);

        return createdColumnList;
    }

    // Helper method to get row values that are common across all Synapse tables and handlers.
    private Map<String, String> getCommonRowValueMap(ExportSubtask subtask) {
        ExportTask task = subtask.getParentTask();
        Item record = subtask.getOriginalRecord();
        String recordId = record.getString("id");

        // get phone and app info
        PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(record);
        String appVersion = phoneAppVersionInfo.getAppVersion();
        String phoneInfo = phoneAppVersionInfo.getPhoneInfo();

        // construct row
        Map<String, String> rowValueMap = new HashMap<>();
        rowValueMap.put("recordId", recordId);
        rowValueMap.put("healthCode", record.getString("healthCode"));
        rowValueMap.put("externalId", BridgeExporterUtil.sanitizeDdbValue(record, "userExternalId", 128, recordId));

        // Data groups, if present. Sort them in alphabetical order, so they appear consistently in Synapse.
        Set<String> dataGroupSet = record.getStringSet("userDataGroups");
        if (dataGroupSet != null) {
            List<String> dataGroupList = new ArrayList<>();
            dataGroupList.addAll(dataGroupSet);
            Collections.sort(dataGroupList);
            rowValueMap.put("dataGroups", DATA_GROUP_JOINER.join(dataGroupList));
        }

        rowValueMap.put("uploadDate", task.getExporterDate().toString());

        // createdOn as a long epoch millis
        rowValueMap.put("createdOn", String.valueOf(record.getLong("createdOn")));

        rowValueMap.put("appVersion", appVersion);
        rowValueMap.put("phoneInfo", phoneInfo);

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
     * List of Synapse table column model objects, to be used to create both the column models and the Synapse table.
     * This excludes columns common to all Bridge tables defined in COMMON_COLUMN_LIST.
     */
    protected abstract List<ColumnModel> getSynapseTableColumnList();

    /** Get the TSV saved in the task for this handler. */
    protected abstract TsvInfo getTsvInfoForTask(ExportTask task);

    /** Save the TSV into the task for this handler. */
    protected abstract void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo);

    /** Creates a row values for a single row from the given export task. */
    protected abstract Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws IOException, SynapseException;
}
