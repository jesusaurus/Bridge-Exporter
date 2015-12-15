package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.table.ColumnModel;
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
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class AppVersionExportHandlerTest {
    private static final String DUMMY_HEALTH_CODE = "dummy-health-code";
    private static final JsonNode DUMMY_RECORD_DATA = DefaultObjectMapper.INSTANCE.createObjectNode();
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final LocalDate DUMMY_REQUEST_DATE = LocalDate.parse("2015-11-03");
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withDate(DUMMY_REQUEST_DATE).build();
    private static final String TEST_STUDY_ID = "testStudy";
    private static final UploadSchemaKey DUMMY_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId("test-schema").withRevision(17).build();
    private static final long TEST_SYNAPSE_DATA_ACCESS_TEAM_ID = 1337;
    private static final int TEST_SYNAPSE_PRINCIPAL_ID = 123456;
    private static final String TEST_SYNAPSE_PROJECT_ID = "test-synapse-project-id";
    private static final String TEST_SYNAPSE_TABLE_ID = "test-synapse-table-id";

    private byte[] tsvBytes;

    @BeforeMethod
    public void setup() {
        // clear tsvBytes, because TestNG doesn't always do that
        tsvBytes = null;
    }

    @Test
    public void test() throws Exception {
        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn("unittest-exporter-");
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock DDB client
        Table mockSynapseTableMap = mock(Table.class);
        when(mockSynapseTableMap.getItem("tableName", TEST_STUDY_ID + "-appVersion")).thenReturn(null);

        DynamoDB mockDdbClient = mock(DynamoDB.class);
        when(mockDdbClient.getTable("unittest-exporter-SynapseMetaTables")).thenReturn(mockSynapseTableMap);

        // mock file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // mock Synapse helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);

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
        List<ColumnModel> createdColumnList = new ArrayList<>();
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("recordId-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("healthCode-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("externalId-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("uploadDate-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("originalTable-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("appVersion-col-id");
            createdColumnList.add(oneCreatedColumn);
        }
        {
            ColumnModel oneCreatedColumn = new ColumnModel();
            oneCreatedColumn.setId("phoneInfo-col-id");
            createdColumnList.add(oneCreatedColumn);
        }

        ArgumentCaptor<List> columnListCaptor = ArgumentCaptor.forClass(List.class);
        when(mockSynapseHelper.createColumnModelsWithRetry(columnListCaptor.capture())).thenReturn(createdColumnList);

        // mock create table
        TableEntity createdTable = new TableEntity();
        createdTable.setId(TEST_SYNAPSE_TABLE_ID);

        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        when(mockSynapseHelper.createTableWithRetry(tableCaptor.capture())).thenReturn(createdTable);

        // setup manager - This is only used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setDdbClient(mockDdbClient);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);

        // set up task
        ExportTask task = new ExportTask.Builder().withExporterDate(DUMMY_REQUEST_DATE).withMetrics(new Metrics())
                .withRequest(DUMMY_REQUEST).withTmpDir(tmpDir).build();

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(eq(TEST_STUDY_ID),
                same(task));
        doReturn(TEST_SYNAPSE_DATA_ACCESS_TEAM_ID).when(manager).getDataAccessTeamIdForStudy(TEST_STUDY_ID);

        // set up handler
        AppVersionExportHandler handler = new AppVersionExportHandler();
        handler.setManager(manager);
        handler.setStudyId(TEST_STUDY_ID);

        // make subtask
        String metadataJsonText = "{\n" +
                "   \"appVersion\":\"Bridge-EX 2.0\",\n" +
                "   \"phoneInfo\":\"My Debugger\"\n" +
                "}";
        Item record = new Item().withString("healthCode", DUMMY_HEALTH_CODE).withString("id", DUMMY_RECORD_ID)
                .withString("metadata", metadataJsonText)
                .withString("userExternalId", "unsanitized\t\texternal\t\tid");

        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(record).withParentTask(task)
                .withRecordData(DUMMY_RECORD_DATA).withSchemaKey(DUMMY_SCHEMA_KEY).build();

        // execute
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        assertEquals(tsvLineList.get(0), "recordId\thealthCode\texternalId\tuploadDate\toriginalTable\tappVersion\t" +
                "phoneInfo");
        assertEquals(tsvLineList.get(1), DUMMY_RECORD_ID + "\t" + DUMMY_HEALTH_CODE + "\tunsanitized external id\t" +
                DUMMY_REQUEST_DATE + "\t" + DUMMY_SCHEMA_KEY + "\tBridge-EX 2.0\tMy Debugger");

        // Don't bother validating metrics or line counts or even file cleanup. This is all tested in another test.
        // Just worry about the basics (the TSV exists and contains our row) and the Synapse table creation.

        // validate Synapse create column args
        assertEquals(columnListCaptor.getValue(), handler.getSynapseTableColumnList());

        // validate Synapse create table args
        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getName(), TEST_STUDY_ID + "-appVersion");
        assertEquals(table.getParentId(), TEST_SYNAPSE_PROJECT_ID);
        assertEquals(table.getColumnIds(), ImmutableList.of("recordId-col-id", "healthCode-col-id",
                "externalId-col-id", "uploadDate-col-id", "originalTable-col-id", "appVersion-col-id",
                "phoneInfo-col-id"));

        // Similarly, don't bother with validating ACLs. This is tested elsewhere.

        // validate DDB put args
        ArgumentCaptor<Item> ddbPutItemArgCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockSynapseTableMap).putItem(ddbPutItemArgCaptor.capture());

        Item ddbPutItemArg = ddbPutItemArgCaptor.getValue();
        assertEquals(ddbPutItemArg.getString("tableName"), TEST_STUDY_ID + "-appVersion");
        assertEquals(ddbPutItemArg.getString("tableId"), TEST_SYNAPSE_TABLE_ID);
    }
}
