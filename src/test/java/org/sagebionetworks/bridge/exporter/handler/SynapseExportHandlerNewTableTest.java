package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.table.ColumnModel;
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
import org.sagebionetworks.bridge.schema.UploadSchema;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseExportHandlerNewTableTest {
    private List<String> columnIdList;
    private ArgumentCaptor<List> columnListCaptor;
    private SynapseHelper mockSynapseHelper;
    private Table mockSynapseTableMap;
    private List<ColumnModel> sourceColumnModelList;
    private ArgumentCaptor<TableEntity> tableCaptor;
    private ExportTask task;
    private byte[] tsvBytes;

    @BeforeMethod
    public void before() {
        // clear tsvBytes, because TestNG doesn't always do that
        tsvBytes = null;
    }

    private void setup(SynapseExportHandler handler) throws Exception {
        // set up handler
        handler.setStudyId(SynapseExportHandlerTest.TEST_STUDY_ID);

        // This needs to be done first, because lots of stuff reference this, even while we're setting up mocks.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(
                SynapseExportHandlerTest.DUMMY_DDB_PREFIX);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock DDB table - no table registered in Synapse
        Map<String, Item> mockSynapseTableMapInternalMap = new HashMap<>();
        mockSynapseTableMap = mock(Table.class);

        when(mockSynapseTableMap.getItem(eq(handler.getDdbTableKeyName()), any(String.class)))
                .thenAnswer(invocation -> {
                    String tableKeyValue = invocation.getArgumentAt(1, String.class);
                    return mockSynapseTableMapInternalMap.get(tableKeyValue);
                });

        when(mockSynapseTableMap.putItem(any(Item.class))).thenAnswer(invocation -> {
            Item newItem = invocation.getArgumentAt(0, Item.class);
            String tableKeyValue = newItem.getString(handler.getDdbTableKeyName());
            mockSynapseTableMapInternalMap.put(tableKeyValue, newItem);

            // Mockito Answers assume a return value, so return null
            return null;
        });

        // mock DDB client
        DynamoDB mockDdbClient = mock(DynamoDB.class);
        when(mockDdbClient.getTable(SynapseExportHandlerTest.DUMMY_DDB_PREFIX + handler.getDdbTableName())).thenReturn(
                mockSynapseTableMap);

        // mock file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // mock Synapse Helper
        mockSynapseHelper = mock(SynapseHelper.class);

        // mock create columns - all we care about are column names and IDs
        sourceColumnModelList = new ArrayList<>();
        sourceColumnModelList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        sourceColumnModelList.addAll(handler.getSynapseTableColumnList());

        List<ColumnModel> createdColumnModelList = new ArrayList<>();
        columnIdList = new ArrayList<>();
        for (ColumnModel oneColumn : sourceColumnModelList) {
            String columnName = oneColumn.getName();
            String columnId = columnName + "-ID";
            ColumnModel createdColumn = new ColumnModel();
            createdColumn.setName(columnName);
            createdColumn.setId(columnId);
            createdColumnModelList.add(createdColumn);

            columnIdList.add(columnId);
        }

        columnListCaptor = ArgumentCaptor.forClass(List.class);
        when(mockSynapseHelper.createColumnModelsWithRetry(columnListCaptor.capture())).thenReturn(
                createdColumnModelList);

        // mock create table - We only care about Table ID.
        TableEntity createdTable = new TableEntity();
        createdTable.setId(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);

        tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        when(mockSynapseHelper.createTableWithRetry(tableCaptor.capture())).thenReturn(createdTable);

        // mock upload the TSV and capture the upload
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID), notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 1 rows
            return 1;
        });

        // setup manager - This is only used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setDdbClient(mockDdbClient);
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
    }

    private void validateTableCreation(SynapseExportHandler handler) throws Exception {
        // Don't bother validating metrics or line counts or even file cleanup. This is all tested in the normal case.
        // Just worry about Synapse table creation.

        // validate Synapse create column args
        assertEquals(columnListCaptor.getValue(), sourceColumnModelList);

        // validate Synapse create table args
        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getName(), handler.getDdbTableKeyValue());
        assertEquals(table.getParentId(), SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID);
        assertEquals(table.getColumnIds(), columnIdList);

        // validate Synapse set ACLs args
        ArgumentCaptor<AccessControlList> aclCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        verify(mockSynapseHelper).createAclWithRetry(aclCaptor.capture());

        AccessControlList acl = aclCaptor.getValue();
        assertEquals(acl.getId(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);

        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();
        assertEquals(resourceAccessSet.size(), 2);

        boolean hasExporterAccess = false;
        boolean hasTeamAccess = false;
        for (ResourceAccess oneAccess : resourceAccessSet) {
            if (oneAccess.getPrincipalId() == SynapseExportHandlerTest.TEST_SYNAPSE_PRINCIPAL_ID) {
                assertEquals(oneAccess.getAccessType(), SynapseExportHandler.ACCESS_TYPE_ALL);
                hasExporterAccess = true;
            } else if (oneAccess.getPrincipalId() == SynapseExportHandlerTest.TEST_SYNAPSE_DATA_ACCESS_TEAM_ID) {
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
        assertEquals(ddbPutItemArg.getString(handler.getDdbTableKeyName()), handler.getDdbTableKeyValue());
        assertEquals(ddbPutItemArg.getString(SynapseExportHandler.DDB_KEY_TABLE_ID), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
    }

    @Test
    public void genericHandlerTest() throws Exception {
        SynapseExportHandler handler = new TestSynapseHandler();
        setup(handler);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "foo", "single record"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "foo");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "single record");

        validateTableCreation(handler);
    }

    @Test
    public void appVersionExportHandlerTest() throws Exception {
        SynapseExportHandler handler = new AppVersionExportHandler();
        setup(handler);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "originalTable");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1),
                SynapseExportHandlerTest.DUMMY_SCHEMA_KEY.toString());

        validateTableCreation(handler);
    }

    @Test
    public void healthDataExportHandlerTest() throws Exception {
        // Freeform text to attachments is already tested in SynapseExportHandlerTest. Just test simple string and int
        // fields.
        UploadSchema testSchema = new UploadSchema.Builder().withKey(SynapseExportHandlerTest.DUMMY_SCHEMA_KEY)
                .addField("foo", "STRING").addField("bar", "INT").build();

        // Set up handler and test. setSchema() needs to be called before setup, since a lot of the stuff in the
        // handler depends on it, even while we're mocking stuff.
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setSchema(testSchema);
        setup(handler);

        // mock serializeToSynapseType() - We actually call through to the real method. Don't need to mock
        // uploadFromS3ToSynapseFileHandle() because we don't have file handles this time.
        when(mockSynapseHelper.serializeToSynapseType(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        // make subtask
        String recordJsonText = "{\n" +
                "   \"foo\":\"This is a string.\",\n" +
                "   \"bar\":42\n" +
                "}";
        ExportSubtask subtask = SynapseExportHandlerTest.makeSubtask(task, recordJsonText);

        // execute
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "foo", "bar");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "This is a string.", "42");

        validateTableCreation(handler);
    }
}
