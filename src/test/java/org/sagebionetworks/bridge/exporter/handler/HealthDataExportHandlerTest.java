package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;

// This was originally a test of HealthDataExportHandler before it got split into SchemaBased and Schemaless. Most of
// these tests test core logic still in HealthDataExportHandler, so we'll keep this class as is.
public class HealthDataExportHandlerTest {
    private static final List<ColumnModel> MOCK_COLUMN_LIST;

    private static final List<ColumnDefinition> MOCK_COLUMN_DEFINITION;

    static {
        MOCK_COLUMN_DEFINITION = SynapseExportHandlerTest.createTestSynapseColumnDefinitions();
        MOCK_COLUMN_LIST = SynapseExportHandlerTest.createTestSynapseColumnList(MOCK_COLUMN_DEFINITION);
    }

    private static final String FIELD_NAME = "foo-field";
    private static final String FIELD_NAME_TIMEZONE = FIELD_NAME + ".timezone";
    private static final String FIELD_VALUE = "asdf jkl;";
    private static final String RAW_DATA_ATTACHMENT_ID = "my-raw.zip";
    private static final String RAW_DATA_FILEHANDLE_ID = "my-raw-data-filehandle";

    private static final UploadFieldDefinition MULTI_CHOICE_FIELD_DEF = new UploadFieldDefinition().name(FIELD_NAME)
            .type(UploadFieldType.MULTI_CHOICE).multiChoiceAnswerList(ImmutableList.of("foo", "bar", "baz", "true",
                    "42"));
    private static final UploadFieldDefinition OTHER_CHOICE_FIELD_DEF = new UploadFieldDefinition()
            .allowOtherChoices(true).name(FIELD_NAME).type(UploadFieldType.MULTI_CHOICE)
            .multiChoiceAnswerList(ImmutableList.of("one", "two"));

    private SchemaBasedExportHandler handler;
    private BridgeHelper mockBridgeHelper;
    private InMemoryFileHelper mockFileHelper;
    private SynapseHelper mockSynapseHelper;
    private byte[] tsvBytes;

    @Test
    public void copy() {
        List<String> multiChoiceAnswerList = ImmutableList.of("foo", "bar", "baz");

        // Make original field def. Note some of these attribute combinations are impossible. This test is a bit
        // contrived.
        UploadFieldDefinition original = new UploadFieldDefinition();
        original.setName("test-field");
        original.setRequired(true);
        original.setType(UploadFieldType.INLINE_JSON_BLOB);
        original.setAllowOtherChoices(true);
        original.setFileExtension(".test");
        original.setMimeType("application/test");
        original.setMaxLength(512);
        original.setMultiChoiceAnswerList(multiChoiceAnswerList);
        original.setUnboundedText(true);

        // Copy and validate
        UploadFieldDefinition copy = HealthDataExportHandler.copy(original);
        assertEquals(copy.getName(), "test-field");
        assertTrue(copy.getRequired());
        assertEquals(copy.getType(), UploadFieldType.INLINE_JSON_BLOB);
        assertTrue(copy.getAllowOtherChoices());
        assertEquals(copy.getFileExtension(), ".test");
        assertEquals(copy.getMimeType(), "application/test");
        assertEquals(copy.getMaxLength().intValue(), 512);
        assertEquals(copy.getMultiChoiceAnswerList(), multiChoiceAnswerList);
        assertTrue(copy.getUnboundedText());
    }

    // branch coverage
    @Test
    public void nullTimestamp() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME, null);
        assertTrue(rowValueMap.isEmpty());
    }

    // branch coverage
    @Test
    public void jsonNullTimestamp() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                NullNode.instance);
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void invalidTypeTimestamp() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                BooleanNode.TRUE);
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void malformedTimestampString() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new TextNode("Thursday June 9th 2016 @ 4:10pm"));
        assertTrue(rowValueMap.isEmpty());
    }

    @DataProvider(name = "timestampStringDataProvider")
    public Object[][] timestampStringDataProvider() {
        // { timestampString, expectedTimezoneString }
        return new Object[][] {
                { "2016-06-09T12:34:56.789Z", "+0000" },
                { "2016-06-09T01:02:03.004+0900", "+0900" },
                { "2016-06-09T02:03:05.007-0700", "-0700" },
                { "2016-06-09T10:09:08.765+0530", "+0530" },
        };
    }

    @Test(dataProvider = "timestampStringDataProvider")
    public void timestampString(String timestampString, String expectedTimezoneString) {
        long expectedMillis = DateTime.parse(timestampString).getMillis();

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new TextNode(timestampString));

        assertEquals(rowValueMap.size(), 2);
        assertEquals(rowValueMap.get(FIELD_NAME), String.valueOf(expectedMillis));
        assertEquals(rowValueMap.get(FIELD_NAME_TIMEZONE), expectedTimezoneString);
    }

    @Test
    public void epochMillis() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new IntNode(12345));

        assertEquals(rowValueMap.size(), 2);
        assertEquals(rowValueMap.get(FIELD_NAME), "12345");
        assertEquals(rowValueMap.get(FIELD_NAME_TIMEZONE), "+0000");
    }

    // branch coverage
    @Test
    public void nullMultiChoice() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", MULTI_CHOICE_FIELD_DEF,
                null);
        assertTrue(rowValueMap.isEmpty());
    }

    // branch coverage
    @Test
    public void jsonNullMultiChoice() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", MULTI_CHOICE_FIELD_DEF,
                NullNode.instance);
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void invalidTypeMultiChoice() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", MULTI_CHOICE_FIELD_DEF,
                new TextNode("baz"));
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void validMultiChoice() throws Exception {
        // Some of the fields aren't strings, to test robustness and string conversion.
        String answerText = "[\"bar\", true, 42]";
        JsonNode answerNode = DefaultObjectMapper.INSTANCE.readTree(answerText);

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", MULTI_CHOICE_FIELD_DEF,
                answerNode);
        assertEquals(rowValueMap.size(), 5);
        assertEquals(rowValueMap.get("foo-field.foo"), "false");
        assertEquals(rowValueMap.get("foo-field.bar"), "true");
        assertEquals(rowValueMap.get("foo-field.baz"), "false");
        assertEquals(rowValueMap.get("foo-field.true"), "true");
        assertEquals(rowValueMap.get("foo-field.42"), "true");
    }

    // branch coverage: If we're expecting an "other choice", but don't get one, that's fine.
    @Test
    public void noOtherChoice() throws Exception {
        String answerText = "[\"one\"]";
        JsonNode answerNode = DefaultObjectMapper.INSTANCE.readTree(answerText);

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", OTHER_CHOICE_FIELD_DEF,
                answerNode);
        assertEquals(rowValueMap.size(), 2);
        assertEquals(rowValueMap.get("foo-field.one"), "true");
        assertEquals(rowValueMap.get("foo-field.two"), "false");
    }

    @Test
    public void oneOtherChoice() throws Exception {
        String answerText = "[\"one\", \"foo\"]";
        JsonNode answerNode = DefaultObjectMapper.INSTANCE.readTree(answerText);

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", OTHER_CHOICE_FIELD_DEF,
                answerNode);
        assertEquals(rowValueMap.size(), 3);
        assertEquals(rowValueMap.get("foo-field.one"), "true");
        assertEquals(rowValueMap.get("foo-field.two"), "false");
        assertEquals(rowValueMap.get("foo-field.other"), "foo");
    }

    // branch coverage: Test we do something reasonable if there are multiple "other" answers.
    @Test
    public void multipleOtherChoice() throws Exception {
        String answerText = "[\"one\", \"foo\", \"bar\", \"baz\"]";
        JsonNode answerNode = DefaultObjectMapper.INSTANCE.readTree(answerText);

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", OTHER_CHOICE_FIELD_DEF,
                answerNode);
        assertEquals(rowValueMap.size(), 3);
        assertEquals(rowValueMap.get("foo-field.one"), "true");
        assertEquals(rowValueMap.get("foo-field.two"), "false");
        assertEquals(rowValueMap.get("foo-field.other"), "bar, baz, foo");
    }

    // branch coverage: The other choice is silently dropped and logged. Here, we just exercise the code and make sure
    // nothing crashes.
    @Test
    public void otherChoiceNotAllowed() throws Exception {
        String answerText = "[\"foo\", \"bar\", \"one\", \"two\"]";
        JsonNode answerNode = DefaultObjectMapper.INSTANCE.readTree(answerText);

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeMultiChoice("dummy", MULTI_CHOICE_FIELD_DEF,
                answerNode);
        assertEquals(rowValueMap.size(), 5);
        assertEquals(rowValueMap.get("foo-field.foo"), "true");
        assertEquals(rowValueMap.get("foo-field.bar"), "true");
        assertEquals(rowValueMap.get("foo-field.baz"), "false");
        assertEquals(rowValueMap.get("foo-field.true"), "false");
        assertEquals(rowValueMap.get("foo-field.42"), "false");
    }

    // Helper method to set up tests where we export something.
    private void setupTest(int numRows, UploadSchema schema, Study study, List<ColumnModel> expectedColumnList)
            throws Exception {
        // Set up handler with the test schema
        handler = new SchemaBasedExportHandler();
        handler.setSchemaKey(BridgeHelperTest.TEST_SCHEMA_KEY);
        handler.setStudyId(BridgeHelperTest.TEST_STUDY_ID);

        // mock BridgeHelper
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSchema(any(), eq(BridgeHelperTest.TEST_SCHEMA_KEY))).thenReturn(schema);
        when(mockBridgeHelper.getStudy(BridgeHelperTest.TEST_STUDY_ID)).thenReturn(study);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(
                SynapseExportHandlerTest.DUMMY_DDB_PREFIX);

        // mock file helper
        mockFileHelper = new InMemoryFileHelper();

        // mock Synapse helper
        mockSynapseHelper = mock(SynapseHelper.class);
        List<ColumnModel> columnModelList = new ArrayList<>();
        columnModelList.addAll(MOCK_COLUMN_LIST);
        columnModelList.addAll(expectedColumnList);
        columnModelList.add(HealthDataExportHandler.RAW_DATA_COLUMN);
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID))
                .thenReturn(columnModelList);

        // mock serializeToSynapseType() - We actually call through to the real method. Don't need to mock
        // uploadFromS3ToSynapseFileHandle() because we don't have file handles this time.
        when(mockSynapseHelper.serializeToSynapseType(any(), any(), any(), any(), any(), any(), any()))
                .thenCallRealMethod();

        // Mock uploadFromS3ToSynapseFileHandle() for raw data attachment.
        when(mockSynapseHelper.uploadFromS3ToSynapseFileHandle(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID,
                RAW_DATA_ATTACHMENT_ID)).thenReturn(RAW_DATA_FILEHANDLE_ID);

        // mock upload the TSV and capture the upload
        tsvBytes = null;
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID), notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed numRows rows
            return numRows;
        });

        // setup manager - This is only used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setBridgeHelper(mockBridgeHelper);
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);
        manager.setSynapseColumnDefinitions(MOCK_COLUMN_DEFINITION);
        handler.setManager(manager);

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(
                eq(BridgeHelperTest.TEST_STUDY_ID), any());
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_DATA_ACCESS_TEAM_ID).when(manager).getDataAccessTeamIdForStudy(
                BridgeHelperTest.TEST_STUDY_ID);

        // Similarly, spy get/setSynapseTableIdFromDDB.
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID).when(manager).getSynapseTableIdFromDdb(any(),
                eq(handler.getDdbTableName()), eq(handler.getDdbTableKeyName()), eq(handler.getDdbTableKeyValue()));
    }

    // This test is to make sure the handler regularly calls to get the schema instead of holding onto it forever.
    // BridgeHelper itself caches schemas for 5 minutes, so it's safe to always call BridgeHelper for the schema.
    //
    // SynapseExportHandlerTest already tests a lot of stuff in-depth. The purpose of this test it to test the
    // specific interaction with BridgeHelper.
    @Test
    public void updateSchemaTest() throws Exception {
        // Test cases: 2 tasks, 2 records each (4 records total). This tests getting the schema for each record and
        // during task initialization.

        Study study = new Study().identifier(BridgeHelperTest.TEST_STUDY_ID).uploadMetadataFieldDefinitions(null);
        List<ColumnModel> expectedColumnList = ImmutableList.of(BridgeHelperTest.TEST_SYNAPSE_COLUMN);
        setupTest(2, BridgeHelperTest.TEST_SCHEMA, study, expectedColumnList);

        // This is not realistic, but for testing simplicity, all four records will have the same JSON.
        String recordJsonText = "{\n" +
                "   \"" + BridgeHelperTest.TEST_FIELD_NAME + "\":\"" + FIELD_VALUE + "\"\n" +
                "}";
        JsonNode recordJsonNode = DefaultObjectMapper.INSTANCE.readTree(recordJsonText);

        // Both records will have identical test code. Use a loop to avoid duplicating code.
        int numGetSchemaCalls = 0;
        int numTasks = 2;
        for (int i = 0; i < numTasks; i++) {
            // set up tasks - We need separate tasks, because tasks have state.
            File tmpDir = mockFileHelper.createTempDir();
            ExportTask task = new ExportTask.Builder().withExporterDate(SynapseExportHandlerTest.DUMMY_REQUEST_DATE)
                    .withMetrics(new Metrics()).withRequest(SynapseExportHandlerTest.DUMMY_REQUEST).withTmpDir(tmpDir)
                    .build();

            // This test will test raw data.
            Item ddbRecord = SynapseExportHandlerTest.makeDdbRecord()
                    .withString(HealthDataExportHandler.DDB_KEY_RAW_DATA_ATTACHMENT_ID, RAW_DATA_ATTACHMENT_ID);

            // However, for the purposes of our tests, we can re-use subtasks.
            ExportSubtask subtask = new ExportSubtask.Builder()
                    .withOriginalRecord(ddbRecord)
                    .withParentTask(task).withRecordData(recordJsonNode).withSchemaKey(BridgeHelperTest.TEST_SCHEMA_KEY)
                    .withStudyId(BridgeHelperTest.TEST_STUDY_ID).build();

            // execute record 1 - This should have 2 calls to getSchema(), one for TSV initialization, one for the
            // record.
            handler.handle(subtask);
            numGetSchemaCalls += 2;
            verify(mockBridgeHelper, times(numGetSchemaCalls)).getSchema(any(), eq(BridgeHelperTest.TEST_SCHEMA_KEY));

            // execute record 2 - This should have another call to getSchema().
            handler.handle(subtask);
            numGetSchemaCalls++;
            verify(mockBridgeHelper, times(numGetSchemaCalls)).getSchema(any(), eq(BridgeHelperTest.TEST_SCHEMA_KEY));

            // Upload table and validate tsv file
            handler.uploadToSynapseForTask(task);
            List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
            assertEquals(tsvLineList.size(), 3);
            SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), BridgeHelperTest.TEST_FIELD_NAME,
                    HealthDataExportHandler.COLUMN_NAME_RAW_DATA);
            SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), FIELD_VALUE, RAW_DATA_FILEHANDLE_ID);
            SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(2), FIELD_VALUE, RAW_DATA_FILEHANDLE_ID);
        }

        // Sanity check to make sure we have the expected number of getSchema calls.
        assertEquals(numGetSchemaCalls, 6);

        // Verify calls to upload raw data.
        verify(mockSynapseHelper, atLeastOnce()).uploadFromS3ToSynapseFileHandle(
                SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID, RAW_DATA_ATTACHMENT_ID);
    }

    // Similarly, this test primarily tests upload metadata. Most of the other stuff is tested in other tests.
    @Test
    public void metadataTest() throws Exception {
        // We only have 1 row and 1 task in our test, but we will have 4 metadata fields to cover everything:
        // 1. multi-choice field
        // 2. timestamp field
        // 3. single-column field
        // 4. field with name conflict

        // Make study w/ metadata fields and schema for test.
        List<UploadFieldDefinition> metadataFieldDefList = ImmutableList.of(
                new UploadFieldDefinition().name("choose-one").type(UploadFieldType.MULTI_CHOICE)
                        .multiChoiceAnswerList(ImmutableList.of("A", "B", "C")).allowOtherChoices(true),
                new UploadFieldDefinition().name("when").type(UploadFieldType.TIMESTAMP),
                new UploadFieldDefinition().name("foo").type(UploadFieldType.INT),
                new UploadFieldDefinition().name("bar").type(UploadFieldType.INT));
        Study study = new Study().identifier(BridgeHelperTest.TEST_STUDY_ID).uploadMetadataFieldDefinitions(
                metadataFieldDefList);

        List<UploadFieldDefinition> schemaFieldDefList = ImmutableList.of(
                new UploadFieldDefinition().name("metadata.bar").type(UploadFieldType.STRING).maxLength(12),
                new UploadFieldDefinition().name("record.foo").type(UploadFieldType.STRING).maxLength(12),
                new UploadFieldDefinition().name("record.baz").type(UploadFieldType.STRING).maxLength(12));
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().fieldDefinitions(schemaFieldDefList);

        // Make expected column list.
        List<ColumnModel> expectedColumnList = new ArrayList<>();
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.choose-one.A");
            col.setColumnType(ColumnType.BOOLEAN);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.choose-one.B");
            col.setColumnType(ColumnType.BOOLEAN);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.choose-one.C");
            col.setColumnType(ColumnType.BOOLEAN);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.choose-one.other");
            col.setColumnType(ColumnType.LARGETEXT);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.when");
            col.setColumnType(ColumnType.DATE);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.when.timezone");
            col.setColumnType(ColumnType.STRING);
            col.setMaximumSize(5L);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.foo");
            col.setColumnType(ColumnType.INTEGER);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("metadata.bar");
            col.setColumnType(ColumnType.STRING);
            col.setMaximumSize(12L);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("record.foo");
            col.setColumnType(ColumnType.STRING);
            col.setMaximumSize(12L);
            expectedColumnList.add(col);
        }
        {
            ColumnModel col = new ColumnModel();
            col.setName("record.baz");
            col.setColumnType(ColumnType.STRING);
            col.setMaximumSize(12L);
            expectedColumnList.add(col);
        }

        // setup test
        setupTest(1, schema, study, expectedColumnList);

        // make task
        File tmpDir = mockFileHelper.createTempDir();
        ExportTask task = new ExportTask.Builder().withExporterDate(SynapseExportHandlerTest.DUMMY_REQUEST_DATE)
                .withMetrics(new Metrics()).withRequest(SynapseExportHandlerTest.DUMMY_REQUEST).withTmpDir(tmpDir)
                .build();

        // make record
        ObjectNode recordNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        recordNode.put("metadata.bar", "metadata-bar");
        recordNode.put("record.foo", "foo-value");
        recordNode.put("record.baz", "baz-value");

        // make DDB item with metadata
        String whenStr = "2017-09-20T16:57:24.130+0900";
        long whenMillis = DateTime.parse(whenStr).getMillis();
        String metadataJsonText = "{\n" +
                "   \"choose-one\":[\"C\", \"None of the above\"],\n" +
                "   \"when\":\"" + whenStr + "\",\n" +
                "   \"foo\":37,\n" +
                "   \"bar\":42\n" +
                "}";
        Item ddbRecord = SynapseExportHandlerTest.makeDdbRecord().withString("userMetadata", metadataJsonText);

        // make subtask
        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(ddbRecord).withParentTask(task)
                .withRecordData(recordNode).withSchemaKey(BridgeHelperTest.TEST_SCHEMA_KEY)
                .withStudyId(BridgeHelperTest.TEST_STUDY_ID).build();

        // execute and validate
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "metadata.choose-one.A",
                "metadata.choose-one.B", "metadata.choose-one.C", "metadata.choose-one.other", "metadata.when",
                "metadata.when.timezone", "metadata.foo", "metadata.bar", "record.foo", "record.baz",
                HealthDataExportHandler.COLUMN_NAME_RAW_DATA);
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "false", "false", "true", "None of the above",
                String.valueOf(whenMillis), "+0900", "37", "metadata-bar", "foo-value", "baz-value",
                SynapseExportHandlerTest.RAW_DATA_FILEHANDLE_ID);

        // Verify calls to upload raw data.
        verify(mockSynapseHelper, atLeastOnce()).uploadFromS3ToSynapseFileHandle(
                SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID, RAW_DATA_ATTACHMENT_ID);
    }
}
