package org.sagebionetworks.bridge.exporter.handler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
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

    // Access types are package-scoped so the unit tests can test against these.
    static final Set<ACCESS_TYPE> ACCESS_TYPE_ALL = ImmutableSet.copyOf(ACCESS_TYPE.values());
    static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);

    private static final Joiner JOINER_COLUMN_SEPARATOR = Joiner.on('\t').useForNull("");

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

            // write to TSV
            List<String> rowValueList = getTsvRowValueList(subtask);
            tsvInfo.writeLine(JOINER_COLUMN_SEPARATOR.join(rowValueList));
            metrics.incrementCounter(tableKey + ".lineCount");
        } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
            metrics.incrementCounter(tableKey + ".errorCount");
            LOG.error("Error processing record " + recordId + " for table " + tableKey + ": " + ex.getMessage(), ex);
        }
    }

    // Gets the TSV for the task, initializing it if it hasn't been created yet
    private TsvInfo initTsvForTask(ExportTask task) throws BridgeExporterException {
        // check if the TSV is already saved in the task
        TsvInfo savedTsvInfo = getTsvInfoForTask(task);
        if (savedTsvInfo != null) {
            return savedTsvInfo;
        }

        try {
            // create TSV and writer
            FileHelper fileHelper = getManager().getFileHelper();
            File tsvFile = fileHelper.newFile(task.getTmpDir(), getDdbTableKeyValue() + ".tsv");
            PrintWriter tsvWriter = new PrintWriter(fileHelper.getWriter(tsvFile));

            // write TSV headers
            List<String> tsvHeaderList = getTsvHeaderList();
            tsvWriter.println(JOINER_COLUMN_SEPARATOR.join(tsvHeaderList));

            // create TSV info
            TsvInfo tsvInfo = new TsvInfo(tsvFile, tsvWriter);
            setTsvInfoForTask(task, tsvInfo);
            return tsvInfo;
        } catch (FileNotFoundException ex) {
            throw new BridgeExporterException("Error initializing TSV: " + ex.getMessage(), ex);
        }
    }

    /**
     * This is called at the end of the record stream for a given export task. This will then upload the TSV to
     * Synapse, creating the Synapse table first if necessary.
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
            // We only create the Synapse table if we actually have lines to update.
            String synapseTableId = initSynapseTable(task);

            // Actually upload the data.
            String projectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
            long linesProcessed = manager.getSynapseHelper().uploadTsvFileToTable(projectId, synapseTableId,
                    tsvFile);
            if (linesProcessed != lineCount) {
                throw new BridgeExporterException("Wrong number of lines processed importing to table=" +
                        synapseTableId + ", expected=" + lineCount + ", actual=" + linesProcessed);
            }

            LOG.info("Done uploading to Synapse for table name=" + getDdbTableKeyValue() + ", id=" + synapseTableId);
        }

        // We've successfully processed the file. We can delete the file now.
        manager.getFileHelper().deleteFile(tsvFile);
    }

    // Initialize the Synapse table for this handler and the current task.
    private String initSynapseTable(ExportTask task) throws BridgeExporterException, SynapseException {
        ExportWorkerManager manager = getManager();
        SynapseHelper synapseHelper = manager.getSynapseHelper();

        // check DDB to see if the Synapse table exists
        String ddbPrefix = manager.getExporterDdbPrefixForTask(task);
        Table synapseTableMap = manager.getDdbClient().getTable(ddbPrefix + getDdbTableName());
        Item tableMapItem = synapseTableMap.getItem(getDdbTableKeyName(), getDdbTableKeyValue());
        if (tableMapItem != null) {
            return tableMapItem.getString("tableId");
        }

        // Synapse table doesn't exist. Create it.
        // Create columns
        List<ColumnModel> columnList = getSynapseTableColumnList();
        List<ColumnModel> createdColumnList = synapseHelper.createColumnModelsWithRetry(columnList);
        if (columnList.size() != createdColumnList.size()) {
            throw new BridgeExporterException("Error creating Synapse table " + getDdbTableKeyValue()
                    + ": Tried to create " + columnList.size() + " columns. Actual: " + createdColumnList.size()
                    + " columns.");
        }

        List<String> columnIdList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            columnIdList.add(oneCreatedColumn.getId());
        }

        // create table - Synapse table names must be unique, so use the DDB key value as the name.
        TableEntity synapseTable = new TableEntity();
        synapseTable.setName(getDdbTableKeyValue());
        synapseTable.setParentId(manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task));
        synapseTable.setColumnIds(columnIdList);
        TableEntity createdTable = synapseHelper.createTableWithRetry(synapseTable);
        String synapseTableId = createdTable.getId();

        // create ACLs
        Set<ResourceAccess> resourceAccessSet = new HashSet<>();

        ResourceAccess exporterOwnerAccess = new ResourceAccess();
        exporterOwnerAccess.setPrincipalId(manager.getSynapsePrincipalId());
        exporterOwnerAccess.setAccessType(ACCESS_TYPE_ALL);
        resourceAccessSet.add(exporterOwnerAccess);

        ResourceAccess dataAccessTeamAccess = new ResourceAccess();
        dataAccessTeamAccess.setPrincipalId(manager.getDataAccessTeamIdForStudy(getStudyId()));
        dataAccessTeamAccess.setAccessType(ACCESS_TYPE_READ);
        resourceAccessSet.add(dataAccessTeamAccess);

        AccessControlList acl = new AccessControlList();
        acl.setId(synapseTableId);
        acl.setResourceAccess(resourceAccessSet);
        synapseHelper.createAclWithRetry(acl);

        // write back to DDB table
        Item synapseTableNewItem = new Item();
        synapseTableNewItem.withString(getDdbTableKeyName(), getDdbTableKeyValue());
        synapseTableNewItem.withString("tableId", synapseTableId);
        synapseTableMap.putItem(synapseTableNewItem);

        return synapseTableId;
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
     * List of Synapse table column model objets, to be used to create both the column models and the Synapse table.
     */
    protected abstract List<ColumnModel> getSynapseTableColumnList();

    /** List of header names for TSV creation. This should match with Synapse table column names. */
    protected abstract List<String> getTsvHeaderList();

    /** Get the TSV saved in the task for this handler. */
    protected abstract TsvInfo getTsvInfoForTask(ExportTask task);

    /** Save the TSV into the task for this handler. */
    protected abstract void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo);

    /** Creates a row values for a single row from the given export task. */
    protected abstract List<String> getTsvRowValueList(ExportSubtask subtask) throws IOException, SynapseException;
}
