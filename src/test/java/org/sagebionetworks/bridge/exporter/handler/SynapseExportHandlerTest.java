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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.exporter.helper.ExportHelper;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

public class SynapseExportHandlerTest {
    // Constants needed to create metadata (phone info, app version)
    private static final String DUMMY_APP_VERSION = "Bridge-EX 2.0";
    private static final String DUMMY_PHONE_INFO = "My Debugger";
    private static final String DUMMY_METADATA_JSON_TEXT = "{\n" +
            "   \"appVersion\":\"" + DUMMY_APP_VERSION +"\",\n" +
            "   \"phoneInfo\":\"" + DUMMY_PHONE_INFO + "\"\n" +
            "}";

    // Constants needed to create a record
    private static final long DUMMY_CREATED_ON = 7777777;
    private static final Set<String> DUMMY_DATA_GROUPS = ImmutableSet.of("foo", "bar", "baz");
    private static final String DUMMY_DATA_GROUPS_FLATTENED = "bar,baz,foo";
    private static final String DUMMY_HEALTH_CODE = "dummy-health-code";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    public static final Item DUMMY_RECORD = new Item().withLong("createdOn", DUMMY_CREATED_ON)
            .withString("healthCode", DUMMY_HEALTH_CODE).withString("id", DUMMY_RECORD_ID)
            .withString("metadata", DUMMY_METADATA_JSON_TEXT).withStringSet("userDataGroups", DUMMY_DATA_GROUPS)
            .withString("userExternalId", "unsanitized\t\texternal\t\tid");

    // Constants to make a request.
    public static final LocalDate DUMMY_REQUEST_DATE = LocalDate.parse("2015-10-31");
    public static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withDate(DUMMY_REQUEST_DATE).build();

    // Constants to make a schema. In most tests, schema doesn't matter. However, in one particular test, namely the
    // test for our old hack to convert freeform text to attachments, we key off specific studies and schemas. This
    // isn't ideal for a test, but we need to test it.
    public static final String TEST_STUDY_ID = "breastcancer";
    public static final String TEST_SCHEMA_ID = "BreastCancer-DailyJournal";
    public static final int TEST_SCHEMA_REV = 1;
    public static final UploadSchemaKey DUMMY_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).build();

    // Misc test constants. Some are shared with other tests.
    private static final String DUMMY_ATTACHMENT_ID = "dummy-attachment-id";
    public static final String DUMMY_DDB_PREFIX = "unittest-exporter-";
    private static final String DUMMY_EXTERNAL_ID = "unsanitized external id";
    private static final String DUMMY_FILEHANDLE_ID = "dummy-filehandle-id";
    private static final String DUMMY_FREEFORM_TEXT_CONTENT = "dummy freeform text content";
    private static final String FREEFORM_FIELD_NAME = "DailyJournalStep103_data.content";
    public static final long TEST_SYNAPSE_DATA_ACCESS_TEAM_ID = 1337;
    public static final int TEST_SYNAPSE_PRINCIPAL_ID = 123456;
    public static final String TEST_SYNAPSE_PROJECT_ID = "test-synapse-project-id";
    public static final String TEST_SYNAPSE_TABLE_ID = "test-synapse-table-id";

    private InMemoryFileHelper mockFileHelper;
    private SynapseHelper mockSynapseHelper;
    private byte[] tsvBytes;
    private ExportTask task;

    @BeforeMethod
    public void before() {
        // clear tsvBytes, because TestNG doesn't always do that
        tsvBytes = null;
    }

    private void setup(SynapseExportHandler handler) throws Exception {
        setupWithSchema(handler, null, null);
    }

    private void setupWithSchema(SynapseExportHandler handler, UploadSchemaKey schemaKey, UploadSchema schema)
    throws Exception {
        // This needs to be done first, because lots of stuff reference this, even while we're setting up mocks.
        handler.setStudyId(TEST_STUDY_ID);

        // mock BridgeHelper
        BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSchema(any(), eq(schemaKey))).thenReturn(schema);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(DUMMY_DDB_PREFIX);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock file helper
        mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // Mock Synapse Helper - We'll fill in the behavior later, because due to the way this test is constructed, we
        // need to set up the Manager before we can properly mock the Synapse Helper.
        mockSynapseHelper = mock(SynapseHelper.class);

        // setup manager - This is mostly used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setBridgeHelper(mockBridgeHelper);
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);
        handler.setManager(manager);

        // set up task
        task = new ExportTask.Builder().withExporterDate(DUMMY_REQUEST_DATE).withMetrics(new Metrics())
                .withRequest(DUMMY_REQUEST).withTmpDir(tmpDir).build();

        // mock Synapse helper
        List<ColumnModel> columnModelList = new ArrayList<>();
        columnModelList.addAll(SynapseExportHandler.COMMON_COLUMN_LIST);
        columnModelList.addAll(handler.getSynapseTableColumnList(task));
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(TEST_SYNAPSE_TABLE_ID)).thenReturn(columnModelList);

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(eq(TEST_STUDY_ID),
                same(task));
        doReturn(1337L).when(manager).getDataAccessTeamIdForStudy(TEST_STUDY_ID);

        // Similarly, spy get/setSynapseTableIdFromDDB.
        doReturn(TEST_SYNAPSE_TABLE_ID).when(manager).getSynapseTableIdFromDdb(task, handler.getDdbTableName(),
                handler.getDdbTableKeyName(), handler.getDdbTableKeyValue());
    }

    @Test
    public void normalCase() throws Exception {
        // This test case will attempt to write 3 rows:
        //   write a line
        //   write error
        //   write 2nd line after error

        SynapseExportHandler handler = new TestSynapseHandler();
        setup(handler);
        mockSynapseHelperUploadTsv(2);

        // make subtasks
        ExportSubtask subtask1 = makeSubtask(task, "foo", "normal first record");
        ExportSubtask subtask2 = makeSubtask(task, "error", "error second record");
        ExportSubtask subtask3 = makeSubtask(task, "foo", "normal third record");

        // execute
        handler.handle(subtask1);
        handler.handle(subtask2);
        handler.handle(subtask3);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 3);
        validateTsvHeaders(tsvLineList.get(0), "foo");
        validateTsvRow(tsvLineList.get(1), "normal first record");
        validateTsvRow(tsvLineList.get(2), "normal third record");

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 2);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 1);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 2);

        postValidation();
    }

    @Test
    public void noRows() throws Exception {
        SynapseExportHandler handler = new TestSynapseHandler();
        setup(handler);

        // execute - We never call the handler with any rows.
        handler.uploadToSynapseForTask(task);

        // verify we don't upload the TSV to Synapse
        verify(mockSynapseHelper, never()).uploadTsvFileToTable(any(), any(), any());

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 0);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 0);

        // validate tsvInfo
        assertNull(handler.getTsvInfoForTask(task));

        postValidation();
    }

    @Test
    public void errorsOnly() throws Exception {
        SynapseExportHandler handler = new TestSynapseHandler();
        setup(handler);

        // make subtasks
        ExportSubtask subtask1 = makeSubtask(task, "error", "first error");
        ExportSubtask subtask2 = makeSubtask(task, "error", "second error");

        // execute
        handler.handle(subtask1);
        handler.handle(subtask2);
        handler.uploadToSynapseForTask(task);

        // verify we don't upload the TSV to Synapse
        verify(mockSynapseHelper, never()).uploadTsvFileToTable(any(), any(), any());

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 0);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 2);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 0);

        postValidation();
    }

    @Test
    public void appVersionExportHandlerTest() throws Exception {
        SynapseExportHandler handler = new AppVersionExportHandler();
        setup(handler);
        mockSynapseHelperUploadTsv(1);

        // make subtasks
        ExportSubtask subtask = makeSubtask(task, "{}");

        // execute
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        validateTsvHeaders(tsvLineList.get(0), "originalTable");
        validateTsvRow(tsvLineList.get(1), DUMMY_SCHEMA_KEY.toString());

        // validate metrics
        Metrics metrics = task.getMetrics();

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 1);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 0);

        SetMultimap<String, String> keyValuesMap = metrics.getKeyValuesMap();
        Set<String> uniqueAppVersionSet = keyValuesMap.get("uniqueAppVersions[" + TEST_STUDY_ID + "]");
        assertTrue(uniqueAppVersionSet.contains(DUMMY_APP_VERSION));

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 1);

        postValidation();
    }

    @Test
    public void healthDataExportHandlerTest() throws Exception {
        // We don't need to exhaustively test all column types, as a lot of it is baked into
        // SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE. We just need to test multi_choice, timestamp,
        // int (non-string), short string, long string (large text aka blob), and freeform text -> attachment
        // Since we want to test that our hack works, we'll need to use the breastcancer-BreastCancer-DailyJournal-v1
        // schema, field DailyJournalStep103_data.content
        UploadSchema testSchema = BridgeHelperTest.simpleSchemaBuilder().withStudyId(TEST_STUDY_ID)
                .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).withFieldDefinitions(
                        new UploadFieldDefinition.Builder().withName("foo").withType(UploadFieldType.STRING)
                                .withMaxLength(20).build(),
                        new UploadFieldDefinition.Builder().withName("foooo").withType(UploadFieldType.STRING)
                                .withMaxLength(9999).build(),
                        new UploadFieldDefinition.Builder().withName("unbounded-foo").withType(UploadFieldType.STRING)
                                .withUnboundedText(true).build(),
                        new UploadFieldDefinition.Builder().withName("bar").withType(UploadFieldType.INT).build(),
                        new UploadFieldDefinition.Builder().withName("submitTime").withType(UploadFieldType.TIMESTAMP)
                                .build(),
                        new UploadFieldDefinition.Builder().withName("sports").withType(UploadFieldType.MULTI_CHOICE)
                                .withMultiChoiceAnswerList("fencing", "football", "running", "swimming").build(),
                        new UploadFieldDefinition.Builder().withName("delicious")
                                .withType(UploadFieldType.MULTI_CHOICE).withMultiChoiceAnswerList("Yes", "No")
                                .withAllowOtherChoices(true).build(),
                        new UploadFieldDefinition.Builder().withName(FREEFORM_FIELD_NAME)
                                .withType(UploadFieldType.STRING).build())
                .build();
        UploadSchemaKey testSchemaKey = BridgeExporterUtil.getSchemaKeyFromSchema(testSchema);

        // Set up handler and test. setSchema() needs to be called before setup, since a lot of the stuff in the
        // handler depends on it, even while we're mocking stuff.
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setSchemaKey(testSchemaKey);
        setupWithSchema(handler, testSchemaKey, testSchema);
        mockSynapseHelperUploadTsv(1);

        // mock export helper
        ExportHelper mockExportHelper = mock(ExportHelper.class);
        when(mockExportHelper.uploadFreeformTextAsAttachment(DUMMY_RECORD_ID, DUMMY_FREEFORM_TEXT_CONTENT))
                .thenReturn(DUMMY_ATTACHMENT_ID);
        handler.getManager().setExportHelper(mockExportHelper);

        // mock serializeToSynapseType() - We actually call through to the real method, but we mock out the underlying
        // uploadFromS3ToSynapseFileHandle() to avoid hitting real back-ends.
        when(mockSynapseHelper.serializeToSynapseType(any(), any(), any(), any(), any())).thenCallRealMethod();

        UploadFieldDefinition freeformAttachmentFieldDef = new UploadFieldDefinition.Builder()
                .withName(FREEFORM_FIELD_NAME).withType(UploadFieldType.ATTACHMENT_V2).build();
        when(mockSynapseHelper.uploadFromS3ToSynapseFileHandle(task.getTmpDir(), TEST_SYNAPSE_PROJECT_ID,
                freeformAttachmentFieldDef, DUMMY_ATTACHMENT_ID)).thenReturn(
                DUMMY_FILEHANDLE_ID);

        // make subtasks
        String submitTimeStr = "2016-06-09T15:54+0900";
        long submitTimeMillis = DateTime.parse(submitTimeStr).getMillis();
        String recordJsonText = "{\n" +
                "   \"foo\":\"This is a string.\",\n" +
                "   \"foooo\":\"Example (not) long string\",\n" +
                "   \"unbounded-foo\":\"Potentially unbounded string\",\n" +
                "   \"bar\":42,\n" +
                "   \"submitTime\":\"" + submitTimeStr + "\",\n" +
                "   \"sports\":[\"fencing\", \"running\"],\n" +
                "   \"delicious\":[\"Yes\", \"No\", \"Maybe\"],\n" +
                "   \"" + FREEFORM_FIELD_NAME + "\":\"" + DUMMY_FREEFORM_TEXT_CONTENT + "\"\n" +
                "}";
        ExportSubtask subtask = makeSubtask(task, recordJsonText);

        // execute
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        validateTsvHeaders(tsvLineList.get(0), "foo", "foooo", "unbounded-foo", "bar", "submitTime",
                "submitTime.timezone", "sports.fencing", "sports.football", "sports.running", "sports.swimming",
                "delicious.Yes", "delicious.No", "delicious.other", FREEFORM_FIELD_NAME);
        validateTsvRow(tsvLineList.get(1), "This is a string.", "Example (not) long string",
                "Potentially unbounded string", "42", String.valueOf(submitTimeMillis), "+0900", "true", "false",
                "true", "false", "true", "true", "Maybe", DUMMY_FILEHANDLE_ID);

        // validate metrics
        Multiset<String> counterMap = task.getMetrics().getCounterMap();
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".lineCount"), 1);
        assertEquals(counterMap.count(handler.getDdbTableKeyValue() + ".errorCount"), 0);

        // validate tsvInfo
        TsvInfo tsvInfo = handler.getTsvInfoForTask(task);
        assertEquals(tsvInfo.getLineCount(), 1);

        postValidation();
    }

    private void mockSynapseHelperUploadTsv(int linesProcessed) throws Exception {
        when(mockSynapseHelper.uploadTsvFileToTable(eq(TEST_SYNAPSE_PROJECT_ID), eq(TEST_SYNAPSE_TABLE_ID),
                notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            return linesProcessed;
        });
    }

    // We do this in a helper method instead of in an @AfterMethod, because @AfterMethod doesn't tell use the test
    // method if it fails.
    private void postValidation() {
        // tmpDir should be the only thing left in the fileHelper. Delete it, then verify isEmpty.
        mockFileHelper.deleteDir(task.getTmpDir());
        assertTrue(mockFileHelper.isEmpty());
    }

    public static ExportSubtask makeSubtask(ExportTask parentTask) throws IOException {
        return makeSubtask(parentTask, "{}");
    }

    public static ExportSubtask makeSubtask(ExportTask parentTask, String key, String value) throws IOException {
        return makeSubtask(parentTask, "{\"" + key + "\":\"" + value + "\"}");
    }

    public static ExportSubtask makeSubtask(ExportTask parentTask, String recordJsonText) throws IOException {
        JsonNode recordJsonNode = DefaultObjectMapper.INSTANCE.readTree(recordJsonText);
        return new ExportSubtask.Builder().withOriginalRecord(DUMMY_RECORD).withParentTask(parentTask)
                .withRecordData(recordJsonNode).withSchemaKey(DUMMY_SCHEMA_KEY).build();
    }

    public static void validateTsvHeaders(String line, String... extraColumnNameVarargs) {
        StringBuilder expectedLineBuilder = new StringBuilder("recordId\thealthCode\texternalId\tdataGroups\t" +
                "uploadDate\tcreatedOn\tappVersion\tphoneInfo");
        for (String oneExtraColumnName : extraColumnNameVarargs) {
            expectedLineBuilder.append('\t');
            expectedLineBuilder.append(oneExtraColumnName);
        }
        assertEquals(line, expectedLineBuilder.toString());
    }

    public static void validateTsvRow(String line, String... extraValueVarargs) {
        StringBuilder expectedLineBuilder = new StringBuilder(DUMMY_RECORD_ID + '\t' + DUMMY_HEALTH_CODE + '\t' +
                DUMMY_EXTERNAL_ID + '\t' + DUMMY_DATA_GROUPS_FLATTENED + '\t' + DUMMY_REQUEST_DATE + '\t' +
                DUMMY_CREATED_ON + '\t' + DUMMY_APP_VERSION + '\t' + DUMMY_PHONE_INFO);
        for (String oneExtraValue : extraValueVarargs) {
            expectedLineBuilder.append('\t');
            expectedLineBuilder.append(oneExtraValue);
        }
        assertEquals(line, expectedLineBuilder.toString());
    }
}
