package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.repo.model.table.ColumnModel;
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
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;

public class HealthDataExportHandlerTest {
    private static List<ColumnModel> MOCK_COLUMN_LIST;

    private static List<ColumnDefinition> MOCK_COLUMN_DEFINITION;

    static {
        MOCK_COLUMN_DEFINITION = SynapseExportHandlerTest.createTestSynapseColumnDefinitions();
        MOCK_COLUMN_LIST = SynapseExportHandlerTest.createTestSynapseColumnList(MOCK_COLUMN_DEFINITION);
    }

    private static final String FIELD_NAME = "foo-field";
    private static final String FIELD_NAME_TIMEZONE = FIELD_NAME + ".timezone";
    private static final String FIELD_VALUE = "asdf jkl;";

    private static final UploadFieldDefinition MULTI_CHOICE_FIELD_DEF = new UploadFieldDefinition().name(FIELD_NAME)
            .type(UploadFieldType.MULTI_CHOICE).multiChoiceAnswerList(ImmutableList.of("foo", "bar", "baz", "true",
                    "42"));
    private static final UploadFieldDefinition OTHER_CHOICE_FIELD_DEF = new UploadFieldDefinition()
            .allowOtherChoices(true).name(FIELD_NAME).type(UploadFieldType.MULTI_CHOICE)
            .multiChoiceAnswerList(ImmutableList.of("one", "two"));

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

    // Needs to be a class member for the next test to work.
    private byte[] tsvBytes;

    // This test is to make sure the handler regularly calls to get the schema instead of holding onto it forever.
    // BridgeHelper itself caches schemas for 5 minutes, so it's safe to always call BridgeHelper for the schema.
    //
    // SynapseExportHandlerTest already tests a lot of stuff in-depth. The purpose of this test it to test the
    // specific interaction with BridgeHelper.
    @Test
    public void updateSchemaTest() throws Exception {
        // Test cases: 2 tasks, 2 records each (4 records total). This tests getting the schema for each record and
        // during task initialization.

        // Set up handler with the test schema
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setSchemaKey(BridgeHelperTest.TEST_SCHEMA_KEY);
        handler.setStudyId(BridgeHelperTest.TEST_STUDY_ID);
        handler.setSynapseColumnDefinitionsAndList(MOCK_COLUMN_DEFINITION);

        // mock BridgeHelper
        BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSchema(any(), eq(BridgeHelperTest.TEST_SCHEMA_KEY))).thenReturn(
                BridgeHelperTest.TEST_SCHEMA);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(
                SynapseExportHandlerTest.DUMMY_DDB_PREFIX);

        // mock file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();

        // mock Synapse helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        List<ColumnModel> columnModelList = new ArrayList<>();
        columnModelList.addAll(MOCK_COLUMN_LIST);
        columnModelList.add(BridgeHelperTest.TEST_SYNAPSE_COLUMN);
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID))
                .thenReturn(columnModelList);

        // mock serializeToSynapseType() - We actually call through to the real method. Don't need to mock
        // uploadFromS3ToSynapseFileHandle() because we don't have file handles this time.
        when(mockSynapseHelper.serializeToSynapseType(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        // mock upload the TSV and capture the upload
        tsvBytes = null;
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID), notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 2 rows
            return 2;
        });

        // setup manager - This is only used to get helper objects.
        ExportWorkerManager manager = spy(new ExportWorkerManager());
        manager.setBridgeHelper(mockBridgeHelper);
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);
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

            // However, for the purposes of our tests, we can re-use subtasks.
            ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(SynapseExportHandlerTest.DUMMY_RECORD)
                    .withParentTask(task).withRecordData(recordJsonNode).withSchemaKey(BridgeHelperTest.TEST_SCHEMA_KEY)
                    .build();

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
            SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), BridgeHelperTest.TEST_FIELD_NAME);
            SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), FIELD_VALUE);
            SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(2), FIELD_VALUE);
        }

        // Sanity check to make sure we have the expected number of getSchema calls.
        assertEquals(numGetSchemaCalls, 6);
    }
}
