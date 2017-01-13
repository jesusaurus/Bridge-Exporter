package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterNonRetryableException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterTsvException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseExportHandlerUpdateTableTest {
    private static List<ColumnModel> MOCK_COLUMN_LIST;

    private static List<ColumnDefinition> MOCK_COLUMN_DEFINITION;

    static {
        MOCK_COLUMN_DEFINITION = SynapseExportHandlerTest.createTestSynapseColumnDefinitions();
        MOCK_COLUMN_LIST = SynapseExportHandlerTest.createTestSynapseColumnList(MOCK_COLUMN_DEFINITION);
    }

    private List<ColumnModel> expectedColDefList;
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

    private SynapseExportHandler setup(List<ColumnModel> existingColumnList) throws Exception {
        // This needs to be done first, because lots of stuff reference this, even while we're setting up mocks.
        SynapseExportHandler handler = new UpdateTestSynapseHandler();
        handler.setSynapseColumnDefinitionsAndList(MOCK_COLUMN_DEFINITION);
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

        // set up task
        task = new ExportTask.Builder().withExporterDate(SynapseExportHandlerTest.DUMMY_REQUEST_DATE)
                .withMetrics(new Metrics()).withRequest(SynapseExportHandlerTest.DUMMY_REQUEST).withTmpDir(tmpDir)
                .build();

        // mock Synapse helper
        mockSynapseHelper = mock(SynapseHelper.class);

        // mock get column model list
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID))
                .thenReturn(existingColumnList);

        // mock create column model list - We only care about the names and IDs for the created columns.
        expectedColDefList = new ArrayList<>();
        expectedColDefList.addAll(MOCK_COLUMN_LIST);
        expectedColDefList.addAll(handler.getSynapseTableColumnList(task));

        List<ColumnModel> createdColumnList = new ArrayList<>();
        expectedColIdList = new ArrayList<>();
        for (ColumnModel oneColumnDef : expectedColDefList) {
            String colName = oneColumnDef.getName();
            String colId = colName + "-id";
            expectedColIdList.add(colId);

            ColumnModel createdColumn = new ColumnModel();
            createdColumn.setName(colName);
            createdColumn.setId(colId);
            createdColumnList.add(createdColumn);
        }

        when(mockSynapseHelper.createColumnModelsWithRetry(anyListOf(ColumnModel.class))).thenReturn(
                createdColumnList);

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

        return handler;
    }

    // Subclass TestSynapseHandler and add a few more fields.
    private static class UpdateTestSynapseHandler extends TestSynapseHandler {
        @Override
        protected List<ColumnModel> getSynapseTableColumnList(ExportTask task) {
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
        // Existing columns has "delete-this" (rejected).
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_COLUMN_LIST);
        existingColumnList.add(makeColumn("delete-this"));
        existingColumnList.add(makeColumn("modify-this"));
        testInitError(existingColumnList);
    }

    @Test
    public void rejectModifyType() throws Exception {
        // Modify column type.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_COLUMN_LIST);

        ColumnModel modifiedTypeColumn = new ColumnModel();
        modifiedTypeColumn.setName("modify-this");
        modifiedTypeColumn.setColumnType(ColumnType.INTEGER);
        existingColumnList.add(modifiedTypeColumn);

        testInitError(existingColumnList);
    }

    private void testInitError(List<ColumnModel> existingColumnList) throws Exception {
        existingColumnList.add(makeColumn("add-this"));
        existingColumnList.add(makeColumn("swap-this-A"));
        existingColumnList.add(makeColumn("swap-this-B"));

        // setup
        SynapseExportHandler handler = setup(existingColumnList);

        // execute - First row triggers the error initializing TSV. Second row short-circuit fails.
        ExportSubtask subtask = SynapseExportHandlerTest.makeSubtask(task);
        handleSubtaskWithInitError(handler, subtask);
        handleSubtaskWithInitError(handler, subtask);

        // upload to Synapse should fail
        try {
            handler.uploadToSynapseForTask(task);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertTrue(ex instanceof BridgeExporterTsvException);
            assertTrue(ex.getMessage().startsWith("TSV was not successfully initialized: "));
            assertTrue(ex.getCause() instanceof BridgeExporterNonRetryableException);
            assertEquals(ex.getCause().getMessage(), "Table has deleted and/or modified columns");
        }

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());

        // verify we don't upload the TSV to Synapse
        verify(mockSynapseHelper, never()).uploadTsvFileToTable(any(), any(), any());

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 0);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 2);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 0);
    }

    private static void handleSubtaskWithInitError(SynapseExportHandler handler, ExportSubtask subtask)
            throws Exception {
        try {
            handler.handle(subtask);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertTrue(ex instanceof BridgeExporterTsvException);
            assertTrue(ex.getMessage().startsWith("TSV was not successfully initialized: "));
            assertTrue(ex.getCause() instanceof BridgeExporterNonRetryableException);
            assertEquals(ex.getCause().getMessage(), "Table has deleted and/or modified columns");
        }
    }

    @Test
    public void dontUpdateIfNoAddedColumns() throws Exception {
        // Swap the columns. "Add this" has already been added.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this"));
        existingColumnList.add(makeColumn("add-this"));
        existingColumnList.add(makeColumn("swap-this-B"));
        existingColumnList.add(makeColumn("swap-this-A"));

        // setup and execute - The columns will be in the order specified by the column defs, not in the order
        // specified in Synapse. This is fine. As long as the headers are properly labeled, Synapse can handle this.
        setupAndExecuteSuccessCase(existingColumnList);

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableWithRetry(any());
    }

    @Test
    public void addAndSwapColumns() throws Exception {
        // Existing columns does not have "add-this" and has swapped columns.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this"));
        existingColumnList.add(makeColumn("swap-this-B"));
        existingColumnList.add(makeColumn("swap-this-A"));

        // setup and execute
        setupAndExecuteSuccessCase(existingColumnList);

        // verify create columns call
        verify(mockSynapseHelper).createColumnModelsWithRetry(expectedColDefList);

        // verify table update
        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        verify(mockSynapseHelper).updateTableWithRetry(tableCaptor.capture());

        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getColumnIds(), expectedColIdList);
    }

    @Test
    public void willAddCommonColumnList() throws Exception {
        // provide an empty existing list and verify if handler will add common column list into it
        List<ColumnModel> existingColumnList = new ArrayList<>();

        setupAndExecuteSuccessCase(existingColumnList);

        // verify create columns call
        verify(mockSynapseHelper).createColumnModelsWithRetry(expectedColDefList);

        // verify table update
        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        verify(mockSynapseHelper).updateTableWithRetry(tableCaptor.capture());

        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getColumnIds(), expectedColIdList);

    }

    @Test
    public void ignoreShrinkingColumn() throws Exception {
        // Existing column is larger. Handler will have a column def with a smaller column.
        testIgnoreResizedColumn(1000);
    }

    @Test
    public void ignoreGrowingColumn() throws Exception {
        testIgnoreResizedColumn(24);
    }

    private void testIgnoreResizedColumn(long oldColumnSize) throws Exception {
        // We need to add a column to trigger the update.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_COLUMN_LIST);

        ColumnModel modifyThisColumn = new ColumnModel();
        modifyThisColumn.setName("modify-this");
        modifyThisColumn.setColumnType(ColumnType.STRING);
        modifyThisColumn.setMaximumSize(oldColumnSize);
        existingColumnList.add(modifyThisColumn);

        existingColumnList.add(makeColumn("swap-this-A"));
        existingColumnList.add(makeColumn("swap-this-B"));

        // setup and execute
        setupAndExecuteSuccessCase(existingColumnList);

        // verify creating the column has the old column size - We only care about the first one "modify-this".
        // "modify-this" is the zeroth column after the commonColumnList, so do some math to get the sizes and
        // indices right.
        ArgumentCaptor<List> submittedColumnListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockSynapseHelper).createColumnModelsWithRetry(submittedColumnListCaptor.capture());

        List<ColumnModel> submittedColumnList = submittedColumnListCaptor.getValue();
        assertEquals(submittedColumnList.size(), MOCK_COLUMN_LIST.size() + 4);

        ColumnModel submittedModifyThisColumn = submittedColumnList.get(
                MOCK_COLUMN_LIST.size());
        assertEquals(submittedModifyThisColumn.getName(), "modify-this");
        assertEquals(submittedModifyThisColumn.getMaximumSize().longValue(), oldColumnSize);

        // verify table update
        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        verify(mockSynapseHelper).updateTableWithRetry(tableCaptor.capture());

        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getColumnIds(), expectedColIdList);
    }

    private void setupAndExecuteSuccessCase(List<ColumnModel> existingColumnList) throws Exception {
        // setup and execute
        SynapseExportHandler handler = setup(existingColumnList);
        handler.handle(SynapseExportHandlerTest.makeSubtask(task));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "modify-this", "add-this", "swap-this-A",
                "swap-this-B");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "modify-this value", "add-this value",
                "swap-this-A value", "swap-this-B value");
    }

    private static ColumnModel makeColumn(String name) {
        ColumnModel col = new ColumnModel();
        col.setName(name);
        col.setColumnType(ColumnType.STRING);
        col.setMaximumSize(100L);
        return col;
    }
}
