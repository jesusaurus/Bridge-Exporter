package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseExportHandlerTest {
    // Records here are only used for logging, so we can share a singleton across tests.
    private static final Item DUMMY_RECORD = new Item().withString("id", "dummy-record-id");

    // These are only needed to ensure valid tasks and subtasks.
    private static final LocalDate DUMMY_REQUEST_DATE = LocalDate.parse("2015-10-31");
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withDate(DUMMY_REQUEST_DATE).build();
    private static final String TEST_STUDY_ID = "testStudy";
    private static final UploadSchemaKey DUMMY_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId("test-schema").withRevision(17).build();

    private static final long TEST_SYNAPSE_DATA_ACCESS_TEAM_ID = 1337;
    private static final int TEST_SYNAPSE_PRINCIPAL_ID = 123456;
    private static final String TEST_SYNAPSE_PROJECT_ID = "test-synapse-project-id";
    private static final String TEST_SYNAPSE_TABLE_ID = "test-synapse-table-id";

    private static class TestSynapseHandler extends SynapseExportHandler {
        private TsvInfo tsvInfo;

        @Override
        protected String getDdbTableName() {
            return "TestDdbTable";
        }

        @Override
        protected String getDdbTableKeyName() {
            return "testId";
        }

        @Override
        protected String getDdbTableKeyValue() {
            return "foobarbaz";
        }

        // The concrete subclass tests will test with multiple columns, so just do something simple for this test.
        @Override
        protected List<ColumnModel> getSynapseTableColumnList() {
            List<ColumnModel> columnList = new ArrayList<>();

            ColumnModel fooColumn = new ColumnModel();
            fooColumn.setName("foo");
            fooColumn.setColumnType(ColumnType.INTEGER);
            columnList.add(fooColumn);
            return columnList;
        }

        // Similarly, only return 1 column here.
        @Override
        protected List<String> getTsvHeaderList() {
            return ImmutableList.of("foo");
        }

        // For test purposes only, we store it directly in the test handler.
        @Override
        protected TsvInfo getTsvInfoForTask(ExportTask task) {
            return tsvInfo;
        }

        @Override
        protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
            this.tsvInfo = tsvInfo;
        }

        // For test purposes, our test data will just be
        // {
        //   "foo":"value"
        // }
        //
        // We write that value to our string list (of size 1, because we have only 1 column).
        //
        // However, if we see the "error" key, throw an IOException with that error message. This is to test
        // error handling.
        @Override
        protected List<String> getTsvRowValueList(ExportSubtask subtask) throws IOException {
            JsonNode dataNode = subtask.getRecordData();
            if (dataNode.has("error")) {
                throw new IOException(dataNode.get("error").textValue());
            }

            String value = dataNode.get("foo").textValue();
            return ImmutableList.of(value);
        }
    }

    private InMemoryFileHelper mockFileHelper;
    private TestSynapseHandler handler;
    private ExportWorkerManager manager;
    private byte[] tsvBytes;
    private ExportTask task;

    @BeforeMethod
    public void setup() throws Exception {
        // clear tsvBytes, because TestNG doesn't always do that
        tsvBytes = null;

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn("unittest-exporter-");
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock file helper
        mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // setup manager - This is only used to get helper objects.
        manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);

        // set up task
        task = new ExportTask.Builder().withExporterDate(DUMMY_REQUEST_DATE).withMetrics(new Metrics())
                .withRequest(DUMMY_REQUEST).withTmpDir(tmpDir).build();

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(eq(TEST_STUDY_ID),
                same(task));
        doReturn(1337L).when(manager).getDataAccessTeamIdForStudy(TEST_STUDY_ID);

        // set up handler
        handler = new TestSynapseHandler();
        handler.setManager(manager);
        handler.setStudyId(TEST_STUDY_ID);
    }

    @Test
    public void normalCase() throws Exception {
        // This test case will attempt to write 3 rows:
        //   write a line
        //   write error
        //   write 2nd line after error

        // set up mock DDB client with table
        manager.setDdbClient(makeMockDdbWithExistingTableId(TEST_SYNAPSE_TABLE_ID));

        // mock Synapse helper - upload the TSV and capture the upload
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.uploadTsvFileToTable(eq(TEST_SYNAPSE_PROJECT_ID), eq(TEST_SYNAPSE_TABLE_ID),
                notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 2 rows
            return 2;
        });
        manager.setSynapseHelper(mockSynapseHelper);

        // make subtasks
        ExportSubtask subtask1 = makeSubtask(task, "normal first record", false);
        ExportSubtask subtask2 = makeSubtask(task, "error second record", true);
        ExportSubtask subtask3 = makeSubtask(task, "normal third record", false);

        // execute
        handler.handle(subtask1);
        handler.handle(subtask2);
        handler.handle(subtask3);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 3);
        assertEquals(tsvLineList.get(0), "foo");
        assertEquals(tsvLineList.get(1), "normal first record");
        assertEquals(tsvLineList.get(2), "normal third record");

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count("foobarbaz.lineCount"), 2);
        assertEquals(counterMap.count("foobarbaz.errorCount"), 1);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 2);

        postValidation();
    }

    @Test
    public void noRows() throws Exception {
        // create a mock DDB client and a mock Synapse client, and then verify that we aren't calling through
        manager.setDdbClient(mock(DynamoDB.class));
        manager.setSynapseHelper(mock(SynapseHelper.class));

        // execute - We never call the handler with any rows.
        handler.uploadToSynapseForTask(task);

        // verify we never called through to DDB or Synapse
        verifyZeroInteractions(manager.getDdbClient(), manager.getSynapseHelper());

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count("foobarbaz.lineCount"), 0);
        assertEquals(counterMap.count("foobarbaz.errorCount"), 0);

        // validate tsvInfo
        assertNull(handler.getTsvInfoForTask(task));

        postValidation();
    }

    @Test
    public void errorsOnly() throws Exception {
        // create a mock DDB client and a mock Synapse client, and then verify that we aren't calling through
        manager.setDdbClient(mock(DynamoDB.class));
        manager.setSynapseHelper(mock(SynapseHelper.class));

        // make subtasks
        ExportSubtask subtask1 = makeSubtask(task, "first error", true);
        ExportSubtask subtask2 = makeSubtask(task, "second error", true);

        // execute
        handler.handle(subtask1);
        handler.handle(subtask2);
        handler.uploadToSynapseForTask(task);

        // verify we never called through to DDB or Synapse
        verifyZeroInteractions(manager.getDdbClient(), manager.getSynapseHelper());

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count("foobarbaz.lineCount"), 0);
        assertEquals(counterMap.count("foobarbaz.errorCount"), 2);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 0);

        postValidation();
    }

    @Test
    public void initSynapseTable() throws Exception {
        // mock DDB client - We do this manually so we can get at the DDB put args
        Table mockSynapseTableMap = mock(Table.class);
        when(mockSynapseTableMap.getItem("testId", "foobarbaz")).thenReturn(null);

        DynamoDB mockDdbClient = mock(DynamoDB.class);
        when(mockDdbClient.getTable("unittest-exporter-TestDdbTable")).thenReturn(mockSynapseTableMap);
        manager.setDdbClient(mockDdbClient);

        // mock Synapse helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        manager.setSynapseHelper(mockSynapseHelper);

        // mock upload the TSV and capture the upload
        when(mockSynapseHelper.uploadTsvFileToTable(eq(TEST_SYNAPSE_PROJECT_ID), eq(TEST_SYNAPSE_TABLE_ID),
                notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 1 rows
            return 1;
        });

        // mock Synapse table creation - For created objects, all we care about is the ID.
        // mock create columns
        ColumnModel createdColumn = new ColumnModel();
        createdColumn.setId("foo-col-id");

        ArgumentCaptor<List> columnListCaptor = ArgumentCaptor.forClass(List.class);
        when(mockSynapseHelper.createColumnModelsWithRetry(columnListCaptor.capture())).thenReturn(
                ImmutableList.of(createdColumn));

        // mock create table
        TableEntity createdTable = new TableEntity();
        createdTable.setId(TEST_SYNAPSE_TABLE_ID);

        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        when(mockSynapseHelper.createTableWithRetry(tableCaptor.capture())).thenReturn(createdTable);

        // execute
        handler.handle(makeSubtask(task, "single record", false));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        assertEquals(tsvLineList.get(0), "foo");
        assertEquals(tsvLineList.get(1), "single record");

        // Don't bother validating metrics or line counts or even file cleanup. This is all tested in the normal case.
        // Just worry about the basics (the TSV exists and contains our row) and the Synapse table creation.

        // validate Synapse create column args
        assertEquals(columnListCaptor.getValue(), handler.getSynapseTableColumnList());

        // validate Synapse create table args
        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getName(), "foobarbaz");
        assertEquals(table.getParentId(), TEST_SYNAPSE_PROJECT_ID);
        assertEquals(table.getColumnIds(), ImmutableList.of("foo-col-id"));

        // validate Synapse set ACLs args
        ArgumentCaptor<AccessControlList> aclCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        verify(mockSynapseHelper).createAclWithRetry(aclCaptor.capture());

        AccessControlList acl = aclCaptor.getValue();
        assertEquals(acl.getId(), TEST_SYNAPSE_TABLE_ID);

        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();
        assertEquals(resourceAccessSet.size(), 2);

        boolean hasExporterAccess = false;
        boolean hasTeamAccess = false;
        for (ResourceAccess oneAccess : resourceAccessSet) {
            if (oneAccess.getPrincipalId() == TEST_SYNAPSE_PRINCIPAL_ID) {
                assertEquals(oneAccess.getAccessType(), SynapseExportHandler.ACCESS_TYPE_ALL);
                hasExporterAccess = true;
            } else if (oneAccess.getPrincipalId() == TEST_SYNAPSE_DATA_ACCESS_TEAM_ID) {
                assertEquals(oneAccess.getAccessType(), SynapseExportHandler.ACCESS_TYPE_READ);
                hasTeamAccess = true;
            } else {
                fail("Unexpected resource access with principal ID " + oneAccess.getPrincipalId());
            }
        }
        assertTrue(hasExporterAccess);
        assertTrue(hasTeamAccess);

        // validate DDB put args
        ArgumentCaptor<Item> ddbPutItemArgCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockSynapseTableMap).putItem(ddbPutItemArgCaptor.capture());

        Item ddbPutItemArg = ddbPutItemArgCaptor.getValue();
        assertEquals(ddbPutItemArg.getString("testId"), "foobarbaz");
        assertEquals(ddbPutItemArg.getString("tableId"), TEST_SYNAPSE_TABLE_ID);
    }

    // We do this in a helper method instead of in an @AfterMethod, because @AfterMethod doesn't tell use the test
    // method if it fails.
    private void postValidation() {
        // tmpDir should be the only thing left in the fileHelper. Delete it, then verify isEmpty.
        mockFileHelper.deleteDir(task.getTmpDir());
        assertTrue(mockFileHelper.isEmpty());
    }

    private static DynamoDB makeMockDdbWithExistingTableId(String tableId) {
        // mock table
        Table mockSynapseTableMap = mock(Table.class);
        when(mockSynapseTableMap.getItem("testId", "foobarbaz")).thenReturn(new Item().withString("tableId", tableId));

        // mock DDB client
        DynamoDB mockDdbClient = mock(DynamoDB.class);
        when(mockDdbClient.getTable("unittest-exporter-TestDdbTable")).thenReturn(mockSynapseTableMap);
        return mockDdbClient;
    }

    private static ExportSubtask makeSubtask(ExportTask parentTask, String data, boolean isError) {
        ObjectNode recordData = DefaultObjectMapper.INSTANCE.createObjectNode().put(isError ? "error" : "foo", data);
        return new ExportSubtask.Builder().withOriginalRecord(DUMMY_RECORD).withParentTask(parentTask)
                .withRecordData(recordData).withSchemaKey(DUMMY_SCHEMA_KEY).build();
    }
}
