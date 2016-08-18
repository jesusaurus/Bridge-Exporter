package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.RestartBridgeExporterException;
import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.HealthDataExportHandler;
import org.sagebionetworks.bridge.exporter.handler.SynapseExportHandler;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseStatusTableHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sqs.SqsHelper;

public class ExportWorkerManagerEndOfStreamTest {
    private static final String DUMMY_JSON_TEXT = "{\"key\":\"value\"}";
    private static final String DUMMY_RECORD_ID_OVERRIDE_BUCKET = "dummy-bucket";
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withDate(LocalDate.parse("2015-12-08")).withTag("test tag").build();

    private static final String DUMMY_SQS_QUEUE_URL = "dummy-sqs-url";

    private ExportWorkerManager manager;
    private List<AppVersionExportHandler> mockAppVersionHandlerList;
    private ExecutorService mockExecutor;
    private List<Future<?>> mockFutureList;
    private List<HealthDataExportHandler> mockHealthDataHandlerList;
    private SynapseStatusTableHelper mockSynapseStatusTableHelper;
    private S3Helper mockS3Helper;
    private SqsHelper mockSqsHelper;

    @BeforeClass
    public void mockTime() {
        DateTimeUtils.setCurrentMillisFixed(DateTime.parse("2016-08-16T01:30:00.001Z").getMillis());
    }

    @AfterClass
    public void cleanupTime() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @BeforeMethod
    public void setup() {
        // Reset future and handler lists.
        mockAppVersionHandlerList = new ArrayList<>();
        mockFutureList = new ArrayList<>();
        mockHealthDataHandlerList = new ArrayList<>();

        // Mock config - Set progress report interval to 2 to test branch coverage
        Config mockConfig = mock(Config.class);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(2);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET)).thenReturn(
                DUMMY_RECORD_ID_OVERRIDE_BUCKET);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_SQS_QUEUE_URL)).thenReturn(DUMMY_SQS_QUEUE_URL);

        // mock helpers - Individual tests can overwrite behavior or verify different behavior.
        mockExecutor = mock(ExecutorService.class);
        mockSynapseStatusTableHelper = mock(SynapseStatusTableHelper.class);
        mockS3Helper = mock(S3Helper.class);
        mockSqsHelper = mock(SqsHelper.class);

        // set up worker manager
        manager = spy(new ExportWorkerManager());
        manager.setConfig(mockConfig);
        manager.setExecutor(mockExecutor);
        manager.setS3Helper(mockS3Helper);
        manager.setSqsHelper(mockSqsHelper);
        manager.setSynapseStatusTableHelper(mockSynapseStatusTableHelper);
    }

    private void mockRecordIdExceptions(Map<String, Exception> recordIdToException) {
        // Mock the executor to create mock futures. This allows us to inject failures into record processing.
        when(mockExecutor.submit(any(ExportWorker.class))).thenAnswer(invocation -> {
            Future<?> mockFuture = mock(Future.class);

            ExportWorker worker = invocation.getArgumentAt(0, ExportWorker.class);
            Exception ex = recordIdToException.get(worker.getSubtask().getRecordId());
            if (ex != null) {
                // Future.get() exceptions are always wrapped in an ExecutionException.
                when(mockFuture.get()).thenThrow(new ExecutionException(ex));
            }

            mockFutureList.add(mockFuture);
            return mockFuture;
        });
    }

    private void mockSchemaIdExceptions(Map<String, Exception> schemaIdToException) throws Exception {
        // Similarly, spy createHealthDataHandler(). This injects failures into the upload TSV step.
        doAnswer(invocation -> {
            HealthDataExportHandler mockHandler = mock(HealthDataExportHandler.class);

            UploadSchemaKey schemaKey = invocation.getArgumentAt(1, UploadSchemaKey.class);
            Exception ex = schemaIdToException.get(schemaKey.getSchemaId());
            if (ex != null) {
                doThrow(ex).when(mockHandler).uploadToSynapseForTask(any());
            }

            mockHealthDataHandlerList.add(mockHandler);
            return mockHandler;
        }).when(manager).createHealthDataHandler(any(), any());
    }

    private void mockStudyIdExceptions(Map<String, Exception> studyIdToException) throws Exception {
        // Similarly, spy createAppVersionHandler(). This injects failures into the upload TSV step of the appVersion
        // table (keyed by study).
        doAnswer(invocation -> {
            AppVersionExportHandler mockHandler = mock(AppVersionExportHandler.class);

            String studyId = invocation.getArgumentAt(0, String.class);
            Exception ex = studyIdToException.get(studyId);
            if (ex != null) {
                doThrow(ex).when(mockHandler).uploadToSynapseForTask(any());
            }

            mockAppVersionHandlerList.add(mockHandler);
            return mockHandler;
        }).when(manager).createAppVersionHandler(any());

        // Similarly, mock SynapseStatusTableHelper to inject failures into the study status table.
        doAnswer(invocation -> {
            String studyId = invocation.getArgumentAt(1, String.class);
            Exception ex = studyIdToException.get(studyId);
            if (ex != null) {
                throw ex;
            }

            // Mockito requires a return value.
            return null;
        }).when(mockSynapseStatusTableHelper).initTableAndWriteStatus(any(), anyString());
    }

    @Test
    public void recoverableFailures() throws Exception {
        // Four records, four tables. A and C fail. B and D succeed.  This allows us to test failure and success cases.
        Item aRecord = new Item().withString("studyId", "study-A").withString("schemaId", "schema-A")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "record-A");
        Item bRecord = new Item().withString("studyId", "study-B").withString("schemaId", "schema-B")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "record-B");
        Item cRecord = new Item().withString("studyId", "study-C").withString("schemaId", "schema-C")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "record-C");
        Item dRecord = new Item().withString("studyId", "study-D").withString("schemaId", "schema-D")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "record-D");

        // mock exceptions A and C fail, for record (future.get), for schema (upload TSV), and for study (appVersion
        // and status table)
        mockRecordIdExceptions(ImmutableMap.<String, Exception>builder().put("record-A", new BridgeExporterException())
                .put("record-C", new BridgeExporterException()).build());
        mockSchemaIdExceptions(ImmutableMap.<String, Exception>builder().put("schema-A", new BridgeExporterException())
                .put("schema-C", new BridgeExporterException()).build());
        mockStudyIdExceptions(ImmutableMap.<String, Exception>builder().put("study-A", new BridgeExporterException())
                .put("study-C", new BridgeExporterException()).build());

        // Create task. We'll want to use a real task here, since it tracks state that we need to test.
        // Study whitelist is mainly there to make sure we're copying over request params to our redrives.
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse("2015-12-08"))
                .withTag("test tag").withStudyWhitelist(ImmutableSet.of("study-A", "study-B", "study-C", "study-D"))
                .build();
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.parse("2015-12-09"))
                .withMetrics(new Metrics()).withRequest(request).withTmpDir(mock(File.class)).build();

        // set up test
        // Record A is executed twice, just to test the dedupe logic in our error handling.
        manager.addSubtaskForRecord(task, aRecord);
        manager.addSubtaskForRecord(task, aRecord);
        manager.addSubtaskForRecord(task, bRecord);
        manager.addSubtaskForRecord(task, cRecord);
        manager.addSubtaskForRecord(task, dRecord);

        // end of stream
        manager.endOfStream(task);

        // verify futures processed (4 records, plus 1 duplicate A, doubled for the appVersion handlers)
        assertEquals(mockFutureList.size(), 10);
        for (Future<?> oneMockFuture : mockFutureList) {
            verify(oneMockFuture).get();
        }

        // verify handlers called to uploaded TSVs (4 tables, 4 studies (appVersion table))
        assertEquals(mockHealthDataHandlerList.size(), 4);
        for (SynapseExportHandler oneMockHandler : mockHealthDataHandlerList) {
            verify(oneMockHandler).uploadToSynapseForTask(task);
        }

        assertEquals(mockAppVersionHandlerList.size(), 4);
        for (SynapseExportHandler oneMockHandler : mockAppVersionHandlerList) {
            verify(oneMockHandler).uploadToSynapseForTask(task);
        }

        // verify status tables written for each study
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "study-A");
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "study-B");
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "study-C");
        verify(mockSynapseStatusTableHelper).initTableAndWriteStatus(task, "study-D");

        // verify redrives
        verify(mockS3Helper).writeLinesToS3(DUMMY_RECORD_ID_OVERRIDE_BUCKET,
                "redrive-record-ids.2016-08-16T01:30:00.001Z", ImmutableSet.of("record-A", "record-C"));

        ArgumentCaptor<BridgeExporterRequest> redriveRequestCaptor = ArgumentCaptor.forClass(
                BridgeExporterRequest.class);
        verify(mockSqsHelper, times(2)).sendMessageAsJson(eq(DUMMY_SQS_QUEUE_URL), redriveRequestCaptor.capture(),
                eq(ExportWorkerManager.REDRIVE_DELAY_SECONDS));
        List<BridgeExporterRequest> redriveRequestList = redriveRequestCaptor.getAllValues();

        BridgeExporterRequest redriveRecordRequest = redriveRequestList.get(0);
        assertNull(redriveRecordRequest.getDate());
        assertNull(redriveRecordRequest.getStartDateTime());
        assertNull(redriveRecordRequest.getEndDateTime());
        assertEquals(redriveRecordRequest.getRecordIdS3Override(), "redrive-record-ids.2016-08-16T01:30:00.001Z");
        assertEquals(redriveRecordRequest.getStudyWhitelist(), request.getStudyWhitelist());
        assertTrue(redriveRecordRequest.getTag().contains("redrive records"));
        assertTrue(redriveRecordRequest.getTag().contains(request.getTag()));

        BridgeExporterRequest redriveTableRequest = redriveRequestList.get(1);
        assertEquals(redriveTableRequest.getDate(), request.getDate());
        assertEquals(redriveTableRequest.getStudyWhitelist(), request.getStudyWhitelist());
        assertTrue(redriveTableRequest.getTag().contains("redrive tables"));
        assertTrue(redriveTableRequest.getTag().contains(request.getTag()));

        Set<UploadSchemaKey> redriveTableWhitelist = redriveTableRequest.getTableWhitelist();
        assertEquals(redriveTableWhitelist.size(), 2);
        assertTrue(redriveTableWhitelist.contains(new UploadSchemaKey.Builder().withStudyId("study-A")
                .withSchemaId("schema-A").withRevision(1).build()));
        assertTrue(redriveTableWhitelist.contains(new UploadSchemaKey.Builder().withStudyId("study-C")
                .withSchemaId("schema-C").withRevision(1).build()));
    }

    @Test
    public void recordFailureSynapse503() throws Exception {
        // "Bad record" fails with a Synapse 503. "Good record" is never processed.
        Item badRecord = new Item().withString("studyId", "test-study").withString("schemaId", "test-schema")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "bad-record");
        Item goodRecord = new Item().withString("studyId", "test-study").withString("schemaId", "test-schema")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "good-record");

        // mock executor to throw 503 on "bad record"
        mockRecordIdExceptions(ImmutableMap.of("bad-record", new SynapseServerException(503, "test exception")));

        // There are no schema (upload TSV) or study (appVersion or status table) errors. However, we should call these
        // to mock out our dependencies.
        mockSchemaIdExceptions(ImmutableMap.of());
        mockStudyIdExceptions(ImmutableMap.of());

        // Create task. New one is created for each test, since it is stateful.
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.parse("2015-12-09"))
                .withMetrics(new Metrics()).withRequest(DUMMY_REQUEST).withTmpDir(mock(File.class)).build();

        // set up test
        manager.addSubtaskForRecord(task, badRecord);
        manager.addSubtaskForRecord(task, goodRecord);

        // end of stream
        try {
            manager.endOfStream(task);
            fail("expected exception");
        } catch (RestartBridgeExporterException ex) {
            assertEquals(ex.getMessage(), "Restarting Bridge Exporter; last recordId=bad-record: test exception");
            SynapseServerException cause = (SynapseServerException) ex.getCause();
            assertEquals(cause.getStatusCode(), 503);
        }

        // We have 4 futures, but only the first one throws and the rest are never executed.
        assertEquals(mockFutureList.size(), 4);
        verify(mockFutureList.get(0)).get();
        for (int i = 1; i < 4; i++) {
            verify(mockFutureList.get(i), never()).get();
        }

        // 1 study, 1 schema, 2 handlers (table, appVersion), but neither one is ever called
        assertEquals(mockHealthDataHandlerList.size(), 1);
        verify(mockHealthDataHandlerList.get(0), never()).uploadToSynapseForTask(any());

        assertEquals(mockAppVersionHandlerList.size(), 1);
        verify(mockAppVersionHandlerList.get(0), never()).uploadToSynapseForTask(any());

        // status tables are never written
        verify(mockSynapseStatusTableHelper, never()).initTableAndWriteStatus(any(), any());

        // no redrives
        verify(mockS3Helper, never()).writeLinesToS3(any(), any(), any());
        verify(mockSqsHelper, never()).sendMessageAsJson(any(), any(), any());
    }

    @Test
    public void tableFailureSynapse503() throws Exception {
        // "Bad schema" upload TSV fails with a 503. "Good schema" is never uploaded. This happens after both records
        // are processed.
        Item badRecord = new Item().withString("studyId", "test-study").withString("schemaId", "bad-schema")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "bad-record");
        Item goodRecord = new Item().withString("studyId", "test-study").withString("schemaId", "good-schema")
                .withInt("schemaRevision", 1).withString("data", DUMMY_JSON_TEXT).withString("id", "good-record");

        // Upload TSV throws Synapse 503 on "bad schema"
        mockSchemaIdExceptions(ImmutableMap.of("bad-schema", new SynapseServerException(503, "test exception")));

        // Similarly, mock out the record and study stuff to mock out our dependencies.
        mockRecordIdExceptions(ImmutableMap.of());
        mockStudyIdExceptions(ImmutableMap.of());

        // Create task. New one is created for each test, since it is stateful.
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.parse("2015-12-09"))
                .withMetrics(new Metrics()).withRequest(DUMMY_REQUEST).withTmpDir(mock(File.class)).build();

        // set up test
        manager.addSubtaskForRecord(task, badRecord);
        manager.addSubtaskForRecord(task, goodRecord);

        // end of stream
        try {
            manager.endOfStream(task);
            fail("expected exception");
        } catch (RestartBridgeExporterException ex) {
            assertEquals(ex.getMessage(),
                    "Restarting Bridge Exporter; last schema=test-study-bad-schema-v1: test exception");
            SynapseServerException cause = (SynapseServerException) ex.getCause();
            assertEquals(cause.getStatusCode(), 503);
        }

        // 2 records = 4 futures (health data and app version). All of these are processed.
        assertEquals(mockFutureList.size(), 4);
        for (Future<?> oneMockFuture : mockFutureList) {
            verify(oneMockFuture).get();
        }

        // 3 handlers (bad-schema, good-schema, appVersion). We know that health data handlers are processed before
        // appVersion handlers, so we know the appVersion handler was never uploaded. However, because we use a hash
        // table, we can make no guarantees on whether good-schema was uploaded before or after bad-schema, and
        // therefore, we don't know whether it was uploaded at all. We do know that our handler list in the test is
        // bad-schema, good schema, so 0 was definitely uploaded (and threw). 1, we're not sure about.
        assertEquals(mockHealthDataHandlerList.size(), 2);
        verify(mockHealthDataHandlerList.get(0)).uploadToSynapseForTask(task);

        assertEquals(mockAppVersionHandlerList.size(), 1);
        verify(mockAppVersionHandlerList.get(0), never()).uploadToSynapseForTask(any());

        // status tables are never written
        verify(mockSynapseStatusTableHelper, never()).initTableAndWriteStatus(any(), any());

        // no redrives
        verify(mockS3Helper, never()).writeLinesToS3(any(), any(), any());
        verify(mockSqsHelper, never()).sendMessageAsJson(any(), any(), any());
    }
}
