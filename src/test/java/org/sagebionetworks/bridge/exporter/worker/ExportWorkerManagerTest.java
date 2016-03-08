package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableMap;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.HealthDataExportHandler;
import org.sagebionetworks.bridge.exporter.handler.IosSurveyExportHandler;
import org.sagebionetworks.bridge.exporter.handler.SynapseExportHandler;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseStatusTableHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

@SuppressWarnings({ "unchecked", "rawtypes" })
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
        verify(mockTask, times(2)).addOutstandingTask(any());
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
        UploadSchema mockSchema = mock(UploadSchema.class);
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSchema(notNull(Metrics.class), eq(testSchemaKey))).thenReturn(mockSchema);

        // set up worker manager
        ExportWorkerManager manager = new ExportWorkerManager();
        manager.setDynamoHelper(mockDynamoHelper);
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
        assertSame(healthDataHandler.getSchema(), mockSchema);
        assertEquals(healthDataHandler.getStudyId(), TEST_STUDY_ID);

        // verify task queue
        verify(mockTask, times(4)).addOutstandingTask(any());

        // verify only one call to DDB
        verify(mockDynamoHelper, times(1)).getSchema(any(), any());
    }

    @Test
    public void endOfStream() throws Exception {
        // Test cases: 2 records, one for "fail-study", one for "success-study". This allows us to test failure and
        // success cases
        Item failRecord = new Item().withString("studyId", "fail-study").withString("schemaId", TEST_SCHEMA_ID)
                .withInt("schemaRevision", TEST_SCHEMA_REV).withString("data", DUMMY_JSON_TEXT);
        Item successRecord = new Item().withString("studyId", "success-study").withString("schemaId", TEST_SCHEMA_ID)
                .withInt("schemaRevision", TEST_SCHEMA_REV).withString("data", DUMMY_JSON_TEXT);

        // mock executor - Don't need to validate the worker. This is done in a previous test.
        ExecutorService mockExecutor = mock(ExecutorService.class);
        List<Future<?>> mockFutureList = new ArrayList<>();
        when(mockExecutor.submit(any(ExportWorker.class))).thenAnswer(invocation -> {
            Future<?> mockFuture = mock(Future.class);

            ExportWorker worker = invocation.getArgumentAt(0, ExportWorker.class);
            if ("fail-study".equals(worker.getSubtask().getSchemaKey().getStudyId())) {
                when(mockFuture.get()).thenThrow(ExecutionException.class);
            }

            mockFutureList.add(mockFuture);
            return mockFuture;
        });

        // Create task. We'll want to use a real task here, since it tracks state that we need to test.
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse("2015-12-08"))
                .build();
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.parse("2015-12-09"))
                .withMetrics(new Metrics()).withRequest(request).withTmpDir(mock(File.class)).build();

        // Mock config - Set progress report interval to 2 to test branch coverage
        Config mockConfig = mock(Config.class);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(2);

        // mock Synapse status table helper
        SynapseStatusTableHelper mockSynapseStatusTableHelper = mock(SynapseStatusTableHelper.class);
        doAnswer(invocation -> {
            String studyId = invocation.getArgumentAt(1, String.class);
            if ("fail-study".equals(studyId)) {
                throw new BridgeExporterException();
            }

            // Mockito requires a return value.
            return null;
        }).when(mockSynapseStatusTableHelper).initTableAndWriteStatus(same(task), anyString());

        // set up worker manager
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setExecutor(mockExecutor);
        manager.setSynapseStatusTableHelper(mockSynapseStatusTableHelper);

        // Spy create*Handler() methods. This allows us to inject failures into the handlers to test handler code.
        List<SynapseExportHandler> mockHandlerList = new ArrayList<>();

        doAnswer(invocation -> {
            AppVersionExportHandler mockHandler = mock(AppVersionExportHandler.class);

            String studyId = invocation.getArgumentAt(0, String.class);
            if ("fail-study".equals(studyId)) {
                doThrow(BridgeExporterException.class).when(mockHandler).uploadToSynapseForTask(any());
            }

            mockHandlerList.add(mockHandler);
            return mockHandler;
        }).when(manager).createAppVersionHandler(any());

        doAnswer(invocation -> {
            HealthDataExportHandler mockHandler = mock(HealthDataExportHandler.class);

            UploadSchemaKey schemaKey = invocation.getArgumentAt(1, UploadSchemaKey.class);
            if ("fail-study".equals(schemaKey.getStudyId())) {
                doThrow(BridgeExporterException.class).when(mockHandler).uploadToSynapseForTask(any());
            }

            mockHandlerList.add(mockHandler);
            return mockHandler;
        }).when(manager).createHealthDataHandler(any(), any());

        // set up test
        manager.addSubtaskForRecord(task, failRecord);
        manager.addSubtaskForRecord(task, successRecord);

        // end of stream
        manager.endOfStream(task);

        // verify futures processed
        assertEquals(mockFutureList.size(), 4);
        for (Future<?> oneMockFuture : mockFutureList) {
            verify(oneMockFuture, times(1)).get();
        }

        // verify handlers called to uploaded TSVs
        assertEquals(mockHandlerList.size(), 4);
        for (SynapseExportHandler oneMockHandler : mockHandlerList) {
            verify(oneMockHandler).uploadToSynapseForTask(task);
        }

        // verify status tables written for each study
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "success-study");
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "fail-study");
    }
}
