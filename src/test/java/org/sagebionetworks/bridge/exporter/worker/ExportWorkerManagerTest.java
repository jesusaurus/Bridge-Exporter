package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParseException;
import com.google.gson.stream.MalformedJsonException;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterNonRetryableException;
import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.HealthDataExportHandler;
import org.sagebionetworks.bridge.exporter.handler.IosSurveyExportHandler;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class ExportWorkerManagerTest {
    private static final String DEFAULT_DDB_PREFIX = "default-prefix-";
    private static final String DUMMY_JSON_TEXT = "{\"key\":\"value\"}";
    private static final String TEST_DDB_KEY_NAME = "test-ddb-key";
    private static final String TEST_DDB_TABLE_NAME = "test-ddb-table";
    private static final String TEST_SCHEMA_ID = "schemaId";
    private static final int TEST_SCHEMA_REV = 3;
    private static final String TEST_STUDY_ID = "studyId";
    private static final String TEST_SYNAPSE_TABLE_ID = "test-Synapse-table";
    private static final String TEST_SYNAPSE_TABLE_NAME = "My Synapse Table";

    private Item ddbSynapseMapItem;

    @BeforeMethod
    public void setup() {
        // clear vars, because TestNG doesn't always do that
        ddbSynapseMapItem = null;
    }

    @DataProvider(name = "ddbPrefixOverrideProvider")
    public Object[][] ddbPrefixOverrideProvider() {
        return new Object[][] {
                { null },
                { "override-prefix-" },
        };
    }

    @Test(dataProvider = "ddbPrefixOverrideProvider")
    public void getSetSynapseTableIdFromDdb(String ddbPrefixOverride) {
        String ddbPrefix = ddbPrefixOverride != null ? ddbPrefixOverride : DEFAULT_DDB_PREFIX;

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(DEFAULT_DDB_PREFIX);

        // mock task
        BridgeExporterRequest mockRequest = mock(BridgeExporterRequest.class);
        when(mockRequest.getExporterDdbPrefixOverride()).thenReturn(ddbPrefixOverride);

        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getRequest()).thenReturn(mockRequest);

        // mock DDB client
        Table mockDdbTable = mock(Table.class);
        ArgumentCaptor<Item> putItemCaptor = ArgumentCaptor.forClass(Item.class);
        when(mockDdbTable.getItem(TEST_DDB_KEY_NAME, TEST_SYNAPSE_TABLE_NAME))
                .thenAnswer(invocation -> ddbSynapseMapItem);
        when(mockDdbTable.putItem(putItemCaptor.capture())).thenAnswer(invocation -> {
            ddbSynapseMapItem = invocation.getArgumentAt(0, Item.class);

            // We don't care about return value, but we require one. Arbitrarily return null.
            return null;
        });

        DynamoDB mockDdbClient = mock(DynamoDB.class);
        when(mockDdbClient.getTable(ddbPrefix + TEST_DDB_TABLE_NAME)).thenReturn(mockDdbTable);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setConfig(mockConfig);
        manager.setDdbClient(mockDdbClient);

        // execute and validate
        // initial get is null
        String retVal1 = manager.getSynapseTableIdFromDdb(mockTask, TEST_DDB_TABLE_NAME, TEST_DDB_KEY_NAME,
                TEST_SYNAPSE_TABLE_NAME);
        assertNull(retVal1);

        // set value
        manager.setSynapseTableIdToDdb(mockTask, TEST_DDB_TABLE_NAME, TEST_DDB_KEY_NAME, TEST_SYNAPSE_TABLE_NAME,
                TEST_SYNAPSE_TABLE_ID);

        // get value back
        String retVal2 = manager.getSynapseTableIdFromDdb(mockTask, TEST_DDB_TABLE_NAME, TEST_DDB_KEY_NAME,
                TEST_SYNAPSE_TABLE_NAME);
        assertEquals(retVal2, TEST_SYNAPSE_TABLE_ID);

        // validate putItemCaptor
        Item putItem = putItemCaptor.getValue();
        assertEquals(putItem.getString(TEST_DDB_KEY_NAME), TEST_SYNAPSE_TABLE_NAME);
        assertEquals(putItem.getString(ExportWorkerManager.DDB_KEY_TABLE_ID), TEST_SYNAPSE_TABLE_ID);
    }

    @Test
    public void getDataAccessTeamIdForStudy() {
        // mock DynamoHelper
        StudyInfo testStudyInfo = new StudyInfo.Builder().withDataAccessTeamId(1337L)
                .withSynapseProjectId("project-id").build();
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudyInfo("test-study")).thenReturn(testStudyInfo);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        long teamId = manager.getDataAccessTeamIdForStudy("test-study");
        assertEquals(teamId, 1337L);
    }

    @Test
    public void getSynapseProjectIdDefault() {
        // mock DynamoHelper
        StudyInfo testStudyInfo = new StudyInfo.Builder().withDataAccessTeamId(1337L)
                .withSynapseProjectId("default-project-id").build();
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudyInfo("test-study")).thenReturn(testStudyInfo);

        // mock task
        BridgeExporterRequest mockRequest = mock(BridgeExporterRequest.class);
        when(mockRequest.getSynapseProjectOverrideMap()).thenReturn(null);

        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getRequest()).thenReturn(mockRequest);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        String projectId = manager.getSynapseProjectIdForStudyAndTask("test-study", mockTask);
        assertEquals(projectId, "default-project-id");
    }

    @Test
    public void getSynapseProjectIdOtherStudyOverridden() {
        // mock DynamoHelper
        StudyInfo testStudyInfo = new StudyInfo.Builder().withDataAccessTeamId(1337L)
                .withSynapseProjectId("default-project-id").build();
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudyInfo("test-study")).thenReturn(testStudyInfo);

        // mock task
        BridgeExporterRequest mockRequest = mock(BridgeExporterRequest.class);
        when(mockRequest.getSynapseProjectOverrideMap()).thenReturn(ImmutableMap.of("other-study",
                "other-project-id"));

        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getRequest()).thenReturn(mockRequest);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        String projectId = manager.getSynapseProjectIdForStudyAndTask("test-study", mockTask);
        assertEquals(projectId, "default-project-id");
    }

    @Test
    public void getSynapseProjectIdOverride() {
        // mock DynamoHelper
        StudyInfo testStudyInfo = new StudyInfo.Builder().withDataAccessTeamId(1337L)
                .withSynapseProjectId("default-project-id").build();
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudyInfo("test-study")).thenReturn(testStudyInfo);

        // mock task
        BridgeExporterRequest mockRequest = mock(BridgeExporterRequest.class);
        when(mockRequest.getSynapseProjectOverrideMap()).thenReturn(ImmutableMap.of("test-study",
                "override-project-id"));

        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getRequest()).thenReturn(mockRequest);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        String projectId = manager.getSynapseProjectIdForStudyAndTask("test-study", mockTask);
        assertEquals(projectId, "override-project-id");
    }

    @Test
    public void addIosSurveySubtask() throws Exception {
        // mock executor
        ArgumentCaptor<ExportWorker> workerCaptor = ArgumentCaptor.forClass(ExportWorker.class);
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.submit(workerCaptor.capture())).thenAnswer(invocation -> mock(Future.class));

        // Mock task. This is passed into the subtask and worker, so we don't need real data in it.
        ExportTask mockTask = mock(ExportTask.class);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setExecutor(mockExecutor);

        // Execute twice. Second time will hit the cache for the survey handler.
        Item record1 = new Item().withString("studyId", TEST_STUDY_ID)
                .withString("schemaId", ExportWorkerManager.SCHEMA_IOS_SURVEY)
                .withInt("schemaRevision", TEST_SCHEMA_REV) .withString("data", DUMMY_JSON_TEXT);
        manager.addSubtaskForRecord(mockTask, record1);
        Item record2 = new Item().withString("studyId", TEST_STUDY_ID)
                .withString("schemaId", ExportWorkerManager.SCHEMA_IOS_SURVEY)
                .withInt("schemaRevision", TEST_SCHEMA_REV) .withString("data", DUMMY_JSON_TEXT);
        manager.addSubtaskForRecord(mockTask, record2);

        // validate subtasks submitted to executor
        verify(mockExecutor, times(2)).submit(any(ExportWorker.class));

        List<ExportWorker> workerList = workerCaptor.getAllValues();
        assertEquals(workerList.size(), 2);

        // Just check the important things, like making sure it corresponds with our task and record.
        IosSurveyExportHandler handler = (IosSurveyExportHandler) workerList.get(0).getHandler();
        assertSame(workerList.get(0).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(0).getSubtask().getOriginalRecord(), record1);

        assertSame(handler, workerList.get(1).getHandler());
        assertSame(workerList.get(1).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(1).getSubtask().getOriginalRecord(), record2);

        // validate handler
        assertSame(handler.getManager(), manager);
        assertEquals(handler.getStudyId(), TEST_STUDY_ID);

        // verify task queue
        verify(mockTask, times(2)).addSubtaskFuture(any());
    }

    @Test
    public void addHealthDataSubtask() throws Exception {
        // mock executor
        ArgumentCaptor<ExportWorker> workerCaptor = ArgumentCaptor.forClass(ExportWorker.class);
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.submit(workerCaptor.capture())).thenAnswer(invocation -> mock(Future.class));

        // Mock task. We only need metrics.
        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getMetrics()).thenReturn(new Metrics());

        // mock DynamoHelper to get schema
        UploadSchemaKey testSchemaKey = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
                .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).build();
        BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSchema(notNull(Metrics.class), eq(testSchemaKey))).thenReturn(
                BridgeHelperTest.TEST_SCHEMA);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setBridgeHelper(mockBridgeHelper);
        manager.setExecutor(mockExecutor);

        // Execute twice. Second time will hit the cache for the app version and health data handlers
        Item record1 = new Item().withString("studyId", TEST_STUDY_ID).withString("schemaId", TEST_SCHEMA_ID)
                .withInt("schemaRevision", TEST_SCHEMA_REV).withString("data", DUMMY_JSON_TEXT);
        manager.addSubtaskForRecord(mockTask, record1);
        Item record2 = new Item().withString("studyId", TEST_STUDY_ID).withString("schemaId", TEST_SCHEMA_ID)
                .withInt("schemaRevision", TEST_SCHEMA_REV).withString("data", DUMMY_JSON_TEXT);
        manager.addSubtaskForRecord(mockTask, record2);

        // validate subtasks submitted to executor (4, 2 for app version, 2 for health data)
        verify(mockExecutor, times(4)).submit(any(ExportWorker.class));

        List<ExportWorker> workerList = workerCaptor.getAllValues();
        assertEquals(workerList.size(), 4);

        // We assume the order is record1 app version, record1 health data, record2 app version, record2 health data.
        // The relative order of app version and health data is probably too tightly coupled to the implementation.
        // However, it's much easier to test if we assume they're in that order.

        // Again, we only need to validate a few important fields in the subtasks.
        AppVersionExportHandler appVersionHandler = (AppVersionExportHandler) workerList.get(0).getHandler();
        assertSame(workerList.get(0).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(0).getSubtask().getOriginalRecord(), record1);

        HealthDataExportHandler healthDataHandler = (HealthDataExportHandler) workerList.get(1).getHandler();
        assertSame(workerList.get(1).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(1).getSubtask().getOriginalRecord(), record1);

        assertSame(workerList.get(2).getHandler(), appVersionHandler);
        assertSame(workerList.get(2).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(2).getSubtask().getOriginalRecord(), record2);

        assertSame(workerList.get(3).getHandler(), healthDataHandler);
        assertSame(workerList.get(3).getSubtask().getParentTask(), mockTask);
        assertSame(workerList.get(3).getSubtask().getOriginalRecord(), record2);

        // validate handlers
        assertSame(appVersionHandler.getManager(), manager);
        assertEquals(appVersionHandler.getStudyId(), TEST_STUDY_ID);

        assertSame(healthDataHandler.getManager(), manager);
        assertEquals(healthDataHandler.getSchemaKey(), testSchemaKey);
        assertEquals(healthDataHandler.getStudyId(), TEST_STUDY_ID);

        // verify task queue
        verify(mockTask, times(4)).addSubtaskFuture(any());

        // verify only one call to DDB
        verify(mockBridgeHelper, times(1)).getSchema(any(), any());
    }

    @DataProvider(name = "isSynapseDownProvider")
    public Object[][] isSynapseDownProvider() {
        // { exception, expected }
        return new Object[][] {
                { new IllegalArgumentException(), false },
                { new BridgeExporterException(), false },
                { new SynapseClientException(), false },
                { new SynapseServerException(404), false },
                { new SynapseServerException(500), false },
                { new SynapseServerException(503), true },

                // branch coverage
                { null, false },
        };
    }

    @Test(dataProvider = "isSynapseDownProvider")
    public void isSynapseDown(Exception exception, boolean expected) {
        assertEquals(ExportWorkerManager.isSynapseDown(exception), expected);
    }

    @SuppressWarnings("serial")
    private static class TestJacksonException extends JsonProcessingException {
        protected TestJacksonException() {
            super("test exception");
        }
    }

    @DataProvider(name = "isRetryableProvider")
    public Object[][] isRetryableProvider() {
        // { exception, expected }
        return new Object[][] {
                { new BadRequestException("test exception", "dummy endpoint"), false },
                { new BridgeSDKException("branch coverage", 310), true },
                { new BridgeSDKException("test 400 exception", 400), false },
                { new BridgeSDKException("test 500 exception", 500), true },
                { new TestJacksonException(), false },
                { new JsonParseException("test exception"), false },
                { new MalformedJsonException("test exception"), false },
                { new BridgeExporterNonRetryableException(), false },
                { new IllegalArgumentException(), true },
                { new BridgeExporterException(), true },

                // branch coverage
                { null, false },
        };
    }

    @Test(dataProvider = "isRetryableProvider")
    public void isRetryable(Exception exception, boolean expected) {
        assertEquals(ExportWorkerManager.isRetryable(exception), expected);
    }
}
