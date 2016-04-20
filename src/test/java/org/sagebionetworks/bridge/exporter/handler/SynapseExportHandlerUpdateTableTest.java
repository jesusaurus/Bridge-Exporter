package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;

public class SynapseExportHandlerUpdateTableTest {
    private List<String> expectedColIdList;
    private SynapseHelper mockSynapseHelper;
    private ExportTask task;
    private byte[] tsvBytes;

    @BeforeMethod
    public void before() {
        // clear vars, because TestNG doesn't always do that
        expectedColIdList = null;
        tsvBytes = null;
    }

    private void setup(SynapseExportHandler handler, List<ColumnModel> existingColumnList) throws Exception {
        // This needs to be done first, because lots of stuff reference this, even while we're setting up mocks.
        handler.setStudyId(SynapseExportHandlerTest.TEST_STUDY_ID);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(
                SynapseExportHandlerTest.DUMMY_DDB_PREFIX);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // mock Synapse helper
        mockSynapseHelper = mock(SynapseHelper.class);

        // mock get column model list
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID))
                .thenReturn(existingColumnList);

        // mock create column model list - We only care about the names and IDs for the created columns.
        List<ColumnModel> columnDefList = new ArrayList<>();
        columnDefList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        columnDefList.addAll(handler.getSynapseTableColumnList());

        List<ColumnModel> createdColumnList = new ArrayList<>();
        expectedColIdList = new ArrayList<>();
        for (ColumnModel oneColumnDef : columnDefList) {
            String colName = oneColumnDef.getName();
            String colId = colName + "-id";
            expectedColIdList.add(colId);

            ColumnModel createdColumn = new ColumnModel();
            createdColumn.setName(colName);
            createdColumn.setId(colId);
            createdColumnList.add(createdColumn);
        }

        when(mockSynapseHelper.createColumnModelsWithRetry(columnDefList)).thenReturn(createdColumnList);

        // mock get table - just a dummy table that we can fill in
        when(mockSynapseHelper.getTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID)).thenReturn(
                new TableEntity());

        // mock upload the TSV and capture the upload
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID), notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 1 rows
            return 1;
        });

        // setup manager - This is mostly used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);
        handler.setManager(manager);


        // set up task
        task = new ExportTask.Builder().withExporterDate(SynapseExportHandlerTest.DUMMY_REQUEST_DATE)
                .withMetrics(new Metrics()).withRequest(SynapseExportHandlerTest.DUMMY_REQUEST).withTmpDir(tmpDir)
                .build();

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(
                eq(SynapseExportHandlerTest.TEST_STUDY_ID), same(task));
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_DATA_ACCESS_TEAM_ID).when(manager).getDataAccessTeamIdForStudy(
                SynapseExportHandlerTest.TEST_STUDY_ID);

        // Similarly, spy get/setSynapseTableIdFromDDB.
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID).when(manager).getSynapseTableIdFromDdb(task,
                handler.getDdbTableName(), handler.getDdbTableKeyName(), handler.getDdbTableKeyValue());
    }

    // Subclass TestSynapseHandler and add a few more fields.
    private static class UpdateTestSynapseHandler extends TestSynapseHandler {
        @Override
        protected List<ColumnModel> getSynapseTableColumnList() {
            return ImmutableList.of(makeColumn("modify-this"), makeColumn("add-this"), makeColumn("swap-this-A"),
                    makeColumn("swap-this-B"));
        }

        // For test purposes, this will always match the schema returned by getSynapseTableColumnList. The tests will
        // validate how this interacts with the "existing" table and updates (or lack thereof).
        @Override
        protected Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws IOException {
            return new ImmutableMap.Builder<String, String>().put("modify-this", "modify-this value")
                    .put("add-this", "add-this value") .put("swap-this-A", "swap-this-A value")
                    .put("swap-this-B", "swap-this-B value").build();
        }
    }

    @Test
    public void rejectDelete() throws Exception {
        // Existing columns has "delete-this" (rejected). Handler has "add-this" (without which we wouldn't trigger the
        // update table code anyway).
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        existingColumnList.add(makeColumn("delete-this"));
        existingColumnList.add(makeColumn("modify-this"));
        existingColumnList.add(makeColumn("swap-this-A"));
        existingColumnList.add(makeColumn("swap-this-B"));

        // setup
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        setup(handler, existingColumnList);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "delete-this", "modify-this", "swap-this-A",
                "swap-this-B");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "", "modify-this value", "swap-this-A value",
                "swap-this-B value");

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());
    }

    @Test
    public void rejectModifyType() throws Exception {
        // Modify column type.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);

        ColumnModel modifiedTypeColumn = new ColumnModel();
        modifiedTypeColumn.setName("modify-this");
        modifiedTypeColumn.setColumnType(ColumnType.INTEGER);
        existingColumnList.add(modifiedTypeColumn);

        existingColumnList.add(makeColumn("swap-this-A"));
        existingColumnList.add(makeColumn("swap-this-B"));

        // setup
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        setup(handler, existingColumnList);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "modify-this", "swap-this-A", "swap-this-B");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "modify-this value", "swap-this-A value",
                "swap-this-B value");

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());
    }

    @Test
    public void rejectModifySize() throws Exception {
        // Modify column size.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);

        ColumnModel modifiedTypeColumn = new ColumnModel();
        modifiedTypeColumn.setName("modify-this");
        modifiedTypeColumn.setColumnType(ColumnType.STRING);
        modifiedTypeColumn.setMaximumSize(42L);
        existingColumnList.add(modifiedTypeColumn);

        existingColumnList.add(makeColumn("swap-this-A"));
        existingColumnList.add(makeColumn("swap-this-B"));

        // setup
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        setup(handler, existingColumnList);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "modify-this", "swap-this-A", "swap-this-B");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "modify-this value", "swap-this-A value",
                "swap-this-B value");

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());
    }

    @Test
    public void dontUpdateIfNoAddedColumns() throws Exception {
        // Swap the columns. "Add this" has already been added.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this"));
        existingColumnList.add(makeColumn("add-this"));
        existingColumnList.add(makeColumn("swap-this-B"));
        existingColumnList.add(makeColumn("swap-this-A"));

        // setup
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        setup(handler, existingColumnList);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file - columns won't be swapped
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "modify-this", "add-this", "swap-this-B",
                "swap-this-A");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "modify-this value", "add-this value",
                "swap-this-B value", "swap-this-A value");

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());
    }

    @Test
    public void addAndSwapColumns() throws Exception {
        // Existing columns does not have "add-this" and has swapped columns.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this"));
        existingColumnList.add(makeColumn("swap-this-B"));
        existingColumnList.add(makeColumn("swap-this-A"));

        // setup
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        setup(handler, existingColumnList);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "modify-this", "add-this", "swap-this-A",
                "swap-this-B");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "modify-this value", "add-this value",
                "swap-this-A value", "swap-this-B value");

        // verify we did not update the table
        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        verify(mockSynapseHelper).updateTableWithRetry(tableCaptor.capture());

        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getColumnIds(), expectedColIdList);
    }

    private static ColumnModel makeColumn(String name) {
        ColumnModel col = new ColumnModel();
        col.setName(name);
        col.setColumnType(ColumnType.STRING);
        col.setMaximumSize(100L);
        return col;
    }
}
