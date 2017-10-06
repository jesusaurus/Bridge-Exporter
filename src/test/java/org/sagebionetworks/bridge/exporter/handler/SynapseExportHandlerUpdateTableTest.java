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
import static org.testng.Assert.assertNull;
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

import org.sagebionetworks.repo.model.table.ColumnChange;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
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
    private static final List<ColumnDefinition> MOCK_COLUMN_DEFINITION;
    private static final List<ColumnModel> MOCK_COLUMN_LIST;
    private static final List<ColumnModel> MOCK_EXISTING_COLUMN_LIST;
    static {
        MOCK_COLUMN_DEFINITION = SynapseExportHandlerTest.createTestSynapseColumnDefinitions();
        MOCK_COLUMN_LIST = SynapseExportHandlerTest.createTestSynapseColumnList(MOCK_COLUMN_DEFINITION);

        // Existing columns are the same, except they also have column IDs.
        MOCK_EXISTING_COLUMN_LIST = new ArrayList<>();
        for (ColumnModel oneColumn : MOCK_COLUMN_LIST) {
            // Copy the columns, so we don't change the columns in MOCK_COLUMN_LIST. The only attributes
            // MOCK_COLUMN_LIST uses are name, type, and max size.
            ColumnModel copy = new ColumnModel();
            copy.setName(oneColumn.getName());
            copy.setColumnType(oneColumn.getColumnType());
            copy.setMaximumSize(oneColumn.getMaximumSize());

            // ID is [name]-id.
            copy.setId(oneColumn.getName() + "-id");

            // Add to list.
            MOCK_EXISTING_COLUMN_LIST.add(copy);
        }
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
        manager.setSynapseColumnDefinitions(MOCK_COLUMN_DEFINITION);
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
            // Columns generated on the fly don't have column IDs yet, because they haven't been created yet.
            return ImmutableList.of(makeColumn("modify-this", null), makeColumn("add-this", null),
                    makeColumn("swap-this-A", null), makeColumn("swap-this-B", null));
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
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);
        existingColumnList.add(makeColumn("delete-this", "delete-this-id"));
        existingColumnList.add(makeColumn("modify-this", "modify-this-id"));
        testInitError(existingColumnList);
    }

    @Test
    public void incompatibleTypeChange() throws Exception {
        // Modify column type.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);

        ColumnModel oldModifiedColumn = new ColumnModel();
        oldModifiedColumn.setName("modify-this");
        oldModifiedColumn.setId("modify-this-old");
        oldModifiedColumn.setColumnType(ColumnType.FILEHANDLEID);
        existingColumnList.add(oldModifiedColumn);

        testInitError(existingColumnList);
    }

    @Test
    public void incompatibleLengthChange() throws Exception {
        // Modify column type.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);

        ColumnModel oldModifiedColumn = new ColumnModel();
        oldModifiedColumn.setName("modify-this");
        oldModifiedColumn.setId("modify-this-old");
        oldModifiedColumn.setColumnType(ColumnType.STRING);
        oldModifiedColumn.setMaximumSize(1000L);
        existingColumnList.add(oldModifiedColumn);

        testInitError(existingColumnList);
    }

    private void testInitError(List<ColumnModel> existingColumnList) throws Exception {
        existingColumnList.add(makeColumn("add-this", "add-this-id"));
        existingColumnList.add(makeColumn("swap-this-A", "swap-this-A-id"));
        existingColumnList.add(makeColumn("swap-this-B", "swap-this-B-id"));

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
        verify(mockSynapseHelper, never()).updateTableColumns(any(), any());

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
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this", "modify-this-id"));
        existingColumnList.add(makeColumn("add-this", "add-this-id"));
        existingColumnList.add(makeColumn("swap-this-B", "swap-this-B-id"));
        existingColumnList.add(makeColumn("swap-this-A", "swap-this-A-id"));

        // setup and execute - The columns will be in the order specified by the column defs, not in the order
        // specified in Synapse. This is fine. As long as the headers are properly labeled, Synapse can handle this.
        setupAndExecuteSuccessCase(existingColumnList);

        // verify we did not update the table
        verify(mockSynapseHelper, never()).updateTableColumns(any(), any());
    }

    @Test
    public void addAndSwapColumns() throws Exception {
        // Existing columns does not have "add-this" and has swapped columns.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);
        existingColumnList.add(makeColumn("modify-this", "modify-this-id"));
        existingColumnList.add(makeColumn("swap-this-B", "swap-this-B-id"));
        existingColumnList.add(makeColumn("swap-this-A", "swap-this-A-id"));

        // setup and execute
        setupAndExecuteSuccessCase(existingColumnList);

        // verify create columns call
        verify(mockSynapseHelper).createColumnModelsWithRetry(expectedColDefList);

        // verify table update
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(mockSynapseHelper).updateTableColumns(requestCaptor.capture(),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), expectedColIdList);

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertNull(changeList.get(0).getOldColumnId());
        assertEquals(changeList.get(0).getNewColumnId(), "add-this-id");
    }

    @Test
    public void willAddCommonColumnList() throws Exception {
        // provide an empty existing list and verify if handler will add common column list into it
        List<ColumnModel> existingColumnList = new ArrayList<>();

        setupAndExecuteSuccessCase(existingColumnList);

        // verify create columns call
        verify(mockSynapseHelper).createColumnModelsWithRetry(expectedColDefList);

        // verify table update
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(mockSynapseHelper).updateTableColumns(requestCaptor.capture(),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), expectedColIdList);

        int numExpectedColumns = expectedColIdList.size();
        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), numExpectedColumns);
        for (int i = 0; i < numExpectedColumns; i++) {
            assertNull(changeList.get(i).getOldColumnId());
            assertEquals(changeList.get(i).getNewColumnId(), expectedColIdList.get(i));
        }
    }

    @Test
    public void compatibleTypeChange() throws Exception {
        // Swap the columns. "Add this" has already been added.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);
        existingColumnList.add(makeColumn("add-this", "add-this-id"));
        existingColumnList.add(makeColumn("swap-this-B", "swap-this-B-id"));
        existingColumnList.add(makeColumn("swap-this-A", "swap-this-A-id"));

        // Old modified is an Int, which can be converted to String just fine.
        ColumnModel oldModifiedColumn = new ColumnModel();
        oldModifiedColumn.setName("modify-this");
        oldModifiedColumn.setId("modify-this-old");
        oldModifiedColumn.setColumnType(ColumnType.INTEGER);
        existingColumnList.add(oldModifiedColumn);

        // setup and execute - The columns will be in the order specified by the column defs, not in the order
        // specified in Synapse. This is fine. As long as the headers are properly labeled, Synapse can handle this.
        setupAndExecuteSuccessCase(existingColumnList);

        // verify table update
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(mockSynapseHelper).updateTableColumns(requestCaptor.capture(),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), expectedColIdList);

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertEquals(changeList.get(0).getOldColumnId(), "modify-this-old");
        assertEquals(changeList.get(0).getNewColumnId(), "modify-this-id");
    }

    @Test
    public void compatibleLengthChange() throws Exception {
        // Swap the columns. "Add this" has already been added.
        List<ColumnModel> existingColumnList = new ArrayList<>();
        existingColumnList.addAll(MOCK_EXISTING_COLUMN_LIST);
        existingColumnList.add(makeColumn("add-this", "add-this-id"));
        existingColumnList.add(makeColumn("swap-this-B", "swap-this-B-id"));
        existingColumnList.add(makeColumn("swap-this-A", "swap-this-A-id"));

        // Old modified is a smaller string (24 chars), which can be converted to a larger string just fine.
        ColumnModel oldModifiedColumn = new ColumnModel();
        oldModifiedColumn.setName("modify-this");
        oldModifiedColumn.setId("modify-this-old");
        oldModifiedColumn.setColumnType(ColumnType.STRING);
        oldModifiedColumn.setMaximumSize(24L);
        existingColumnList.add(oldModifiedColumn);

        // setup and execute - The columns will be in the order specified by the column defs, not in the order
        // specified in Synapse. This is fine. As long as the headers are properly labeled, Synapse can handle this.
        setupAndExecuteSuccessCase(existingColumnList);

        // verify table update
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(mockSynapseHelper).updateTableColumns(requestCaptor.capture(),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), expectedColIdList);

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertEquals(changeList.get(0).getOldColumnId(), "modify-this-old");
        assertEquals(changeList.get(0).getNewColumnId(), "modify-this-id");
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

    private static ColumnModel makeColumn(String name, String id) {
        ColumnModel col = new ColumnModel();
        col.setName(name);
        col.setId(id);
        col.setColumnType(ColumnType.STRING);
        col.setMaximumSize(100L);
        return col;
    }
}
