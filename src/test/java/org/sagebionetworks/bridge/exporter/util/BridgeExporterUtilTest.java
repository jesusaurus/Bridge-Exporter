package org.sagebionetworks.bridge.exporter.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.bridge.exporter.synapse.TransferMethod;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class BridgeExporterUtilTest {
    private List<ColumnModel> TEST_COLUMN_MODELS;
    private List<ColumnDefinition> TEST_COLUMN_DEFINITIONS;
    private static final String TEST_DATE = "2017-01-11";
    private static final LocalDate TEST_LOCAL_DATE = LocalDate.parse(TEST_DATE);


    @Test
    public void getFieldDefMapFromSchema() {
        // set up test schema
        UploadFieldDefinition fooField = new UploadFieldDefinition().name("foo").type(UploadFieldType.STRING);
        UploadFieldDefinition barField = new UploadFieldDefinition().name("bar").type(UploadFieldType.INT);
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().addFieldDefinitionsItem(fooField)
                .addFieldDefinitionsItem(barField);

        // execute and validate
        Map<String, UploadFieldDefinition> fieldDefMap = BridgeExporterUtil.getFieldDefMapFromSchema(schema);
        assertEquals(fieldDefMap.size(), 2);
        assertEquals(fieldDefMap.get("foo"), fooField);
        assertEquals(fieldDefMap.get("bar"), barField);
    }

    @Test
    public void getSchemaKeyForRecord() {
        // mock DDB Health Record
        Item recordItem = new Item().withString("studyId", "test-study").withString("schemaId", "test-schema")
                .withInt("schemaRevision", 13);
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyForRecord(recordItem);
        assertEquals(schemaKey.toString(), "test-study-test-schema-v13");
    }

    @Test
    public void getSchemaKeyFromSchema() {
        // set up test schema
        UploadFieldDefinition fooField = new UploadFieldDefinition().name("foo").type(UploadFieldType.STRING);
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().studyId("test-study").schemaId("test-schema")
                .revision(3L).addFieldDefinitionsItem(fooField);

        // execute and validate
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyFromSchema(schema);
        assertEquals(schemaKey.getStudyId(), "test-study");
        assertEquals(schemaKey.getSchemaId(), "test-schema");
        assertEquals(schemaKey.getRevision(), 3);
    }

    // branch coverage
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp =
            "revision can't be null")
    public void getSchemaKeyNullRev() {
        // set up test schema
        UploadFieldDefinition fooField = new UploadFieldDefinition().name("foo").type(UploadFieldType.STRING);
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().studyId("test-study").schemaId("test-schema")
                .revision(null).addFieldDefinitionsItem(fooField);

        // execute and validate
        BridgeExporterUtil.getSchemaKeyFromSchema(schema);
    }

    @Test
    public void sanitizeDdbValueNoValue() {
        assertNull(BridgeExporterUtil.sanitizeDdbValue(new Item(), "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeDdbValueNormalCase() {
        Item item = new Item().withString("key", "123\t\t\t456");
        String out = BridgeExporterUtil.sanitizeDdbValue(item, "key", 5, "dummy-record");
        assertEquals(out, "123 4");
    }

    @Test
    public void sanitizeJsonValueNotObject() throws Exception {
        String jsonText = "\"not an object\"";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeJsonValueNoValue() throws Exception {
        String jsonText = "{}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeJsonValueNullValue() throws Exception {
        String jsonText = "{\"key\":null}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeJsonValueNotString() throws Exception {
        String jsonText = "{\"key\":42}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeJsonValueNormalCase() throws Exception {
        String jsonText = "{\"key\":\"123\\t\\t\\t456\"}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        String out = BridgeExporterUtil.sanitizeJsonValue(node, "key", 5, "dummy-record");
        assertEquals(out, "123 4");
    }

    @Test
    public void sanitizeStringNull() {
        assertNull(BridgeExporterUtil.sanitizeString(null, 100, "dummy-record"));
    }

    @Test
    public void sanitizeStringEmpty() {
        String out = BridgeExporterUtil.sanitizeString("", 100, "dummy-record");
        assertEquals(out, "");
    }

    @Test
    public void sanitizeStringPassthrough() {
        String out = BridgeExporterUtil.sanitizeString("lorem ipsum", 100, "dummy-record");
        assertEquals(out, "lorem ipsum");
    }

    @Test
    public void sanitizeStringRemoveHtml() {
        String out = BridgeExporterUtil.sanitizeString("<b>bold text</b>", 100, "dummy-record");
        assertEquals(out, "bold text");
    }

    @Test
    public void sanitizeStringRemovePartialHtml() {
        String out = BridgeExporterUtil.sanitizeString("imbalanced</i> <p>tags", 100, "dummy-record");
        assertEquals(out, "imbalanced tags");
    }

    @Test
    public void sanitizeStringRemoveBadChars() {
        String in = "newlines\n\n\n" +
                "CRLF\r\n" +
                "tabs\t\ttabs";
        String out = BridgeExporterUtil.sanitizeString(in, 1000, "dummy-record");
        assertEquals(out, "newlines CRLF tabs tabs");
    }

    @Test
    public void sanitizeStringTooLong() {
        String out = BridgeExporterUtil.sanitizeString("1234567890", 4, "dummy-record");
        assertEquals(out, "1234");
    }

    // branch coverage
    @Test
    public void sanitizeStringNullMaxLength() {
        String out = BridgeExporterUtil.sanitizeString("stuff", null, "dummy-record");
        assertEquals(out, "stuff");
    }

    @Test
    public void shouldConvertFreeformTextToAttachment() {
        // This is a hack, but we still need to test it.
        UploadSchemaKey bcsDailyJournalSchema = new UploadSchemaKey.Builder().withStudyId("breastcancer")
                .withSchemaId("BreastCancer-DailyJournal").withRevision(1).build();
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsDailyJournalSchema,
                "content_data.APHMoodLogNoteText"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsDailyJournalSchema,
                "DailyJournalStep103_data.content"));
        assertFalse(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsDailyJournalSchema,
                "something-else"));

        UploadSchemaKey bcsExerciseSurveyJournal = new UploadSchemaKey.Builder().withStudyId("breastcancer")
                .withSchemaId("BreastCancer-ExerciseSurvey").withRevision(1).build();
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey101_data.result"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey102_data.result"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey103_data.result"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey104_data.result"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey105_data.result"));
        assertTrue(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "exercisesurvey106_data.result"));
        assertFalse(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(bcsExerciseSurveyJournal,
                "something-else"));

        UploadSchemaKey other = new UploadSchemaKey.Builder().withStudyId("other-study").withSchemaId("other-schema")
                .withRevision(1).build();
        assertFalse(BridgeExporterUtil.shouldConvertFreeformTextToAttachment(other, "other"));
    }

    @Test
    public void canConvertToColumnList() {
        final String TEST_COLUMN_MODEL_NAME_1 = "test_column_model_1";
        final String TEST_COLUMN_MODEL_NAME_2 = "test_column_model_2";

        List<ColumnModel> TEST_COLUMN_LIST;

        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel TEST_MODEL_1 = new ColumnModel();
        TEST_MODEL_1.setName(TEST_COLUMN_MODEL_NAME_1);
        TEST_MODEL_1.setColumnType(ColumnType.STRING);
        TEST_MODEL_1.setMaximumSize(36L);
        columnListBuilder.add(TEST_MODEL_1);

        ColumnModel TEST_MODEL_2 = new ColumnModel();
        TEST_MODEL_2.setName(TEST_COLUMN_MODEL_NAME_2);
        TEST_MODEL_2.setColumnType(ColumnType.STRING);
        TEST_MODEL_2.setMaximumSize(48L);
        columnListBuilder.add(TEST_MODEL_2);

        TEST_COLUMN_LIST = columnListBuilder.build();

        List<ColumnDefinition> TEST_COLUMN_DEFINITIONS;

        ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();

        ColumnDefinition TEST_DEFINITION_1 = new ColumnDefinition();
        TEST_DEFINITION_1.setName(TEST_COLUMN_MODEL_NAME_1);
        TEST_DEFINITION_1.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_1.setMaximumSize(36L);
        columnDefinitionBuilder.add(TEST_DEFINITION_1);

        ColumnDefinition TEST_DEFINITION_2 = new ColumnDefinition();
        TEST_DEFINITION_2.setName(TEST_COLUMN_MODEL_NAME_2);
        TEST_DEFINITION_2.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_2.setMaximumSize(48L);
        columnDefinitionBuilder.add(TEST_DEFINITION_2);

        TEST_COLUMN_DEFINITIONS = columnDefinitionBuilder.build();

        assertEquals(BridgeExporterUtil.convertToColumnList(TEST_COLUMN_DEFINITIONS), TEST_COLUMN_LIST);
    }

    @Test
    public void canGetRowValuesFromRecordBasedOnColumnDefinition() {
        final String TEST_STRING_NAME = "test_string";
        final String TEST_STRING_SET_NAME = "test_string_set";
        final String TEST_DATE_NAME = "test_date";
        final String TEST_EXPORTER_DATE_NAME = "test_exporter_date";
        final String TEST_SANITIZE = "test_sanitize";

        // setup mock
        ExportTask mockTask = mock(ExportTask.class);
        when(mockTask.getExporterDate()).thenReturn(TEST_LOCAL_DATE);

        // create mock record
        Item testRecord = new Item();
        testRecord.withString(TEST_STRING_NAME, "test_string_value");
        testRecord.withStringSet(TEST_STRING_SET_NAME, new String[]{"test_string_set_value_1", "test_string_set_value_2"});
        testRecord.withLong(TEST_DATE_NAME, 1484181511);
        testRecord.withString(TEST_EXPORTER_DATE_NAME, "test_exporter_date_value");
        testRecord.with(TEST_SANITIZE, "test_sanitize_value");

        // create expected map
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(TEST_STRING_NAME, "test_string_value");
        expectedMap.put(TEST_STRING_SET_NAME, "test_string_set_value_1,test_string_set_value_2");
        expectedMap.put(TEST_DATE_NAME, "1484181511");
        expectedMap.put(TEST_EXPORTER_DATE_NAME, TEST_LOCAL_DATE.toString());
        expectedMap.put(TEST_SANITIZE, "test_sanitize_value");


        // create mock column definitions
        List<ColumnDefinition> TEST_COLUMN_DEFINITIONS;

        ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();

        ColumnDefinition TEST_DEFINITION_1 = new ColumnDefinition();
        TEST_DEFINITION_1.setName(TEST_STRING_NAME);
        TEST_DEFINITION_1.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_1.setMaximumSize(36L);
        TEST_DEFINITION_1.setTransferMethod(TransferMethod.STRING);
        TEST_DEFINITION_1.setDdbName(TEST_STRING_NAME);
        columnDefinitionBuilder.add(TEST_DEFINITION_1);

        ColumnDefinition TEST_DEFINITION_2 = new ColumnDefinition();
        TEST_DEFINITION_2.setName(TEST_STRING_SET_NAME);
        TEST_DEFINITION_2.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_2.setTransferMethod(TransferMethod.STRINGSET);
        TEST_DEFINITION_2.setDdbName(TEST_STRING_SET_NAME);
        TEST_DEFINITION_2.setMaximumSize(100L);
        columnDefinitionBuilder.add(TEST_DEFINITION_2);

        ColumnDefinition TEST_DEFINITION_3 = new ColumnDefinition();
        TEST_DEFINITION_3.setName(TEST_EXPORTER_DATE_NAME);
        TEST_DEFINITION_3.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_3.setTransferMethod(TransferMethod.EXPORTERDATE);
        TEST_DEFINITION_3.setDdbName(TEST_EXPORTER_DATE_NAME);
        TEST_DEFINITION_3.setMaximumSize(36L);
        columnDefinitionBuilder.add(TEST_DEFINITION_3);

        ColumnDefinition TEST_DEFINITION_4 = new ColumnDefinition();
        TEST_DEFINITION_4.setName(TEST_DATE_NAME);
        TEST_DEFINITION_4.setColumnType(ColumnType.DATE);
        TEST_DEFINITION_4.setTransferMethod(TransferMethod.DATE);
        TEST_DEFINITION_4.setDdbName(TEST_DATE_NAME);
        TEST_DEFINITION_4.setMaximumSize(36L);
        columnDefinitionBuilder.add(TEST_DEFINITION_4);

        ColumnDefinition TEST_DEFINITION_5 = new ColumnDefinition();
        TEST_DEFINITION_5.setName(TEST_SANITIZE);
        TEST_DEFINITION_5.setColumnType(ColumnType.STRING);
        TEST_DEFINITION_5.setMaximumSize(36L);
        TEST_DEFINITION_5.setTransferMethod(TransferMethod.STRING);
        TEST_DEFINITION_5.setDdbName(TEST_SANITIZE);
        TEST_DEFINITION_5.setSanitize(true);
        columnDefinitionBuilder.add(TEST_DEFINITION_5);

        TEST_COLUMN_DEFINITIONS = columnDefinitionBuilder.build();

        // process
        Map<String, String> retMap = BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(new HashMap<>(), testRecord, TEST_COLUMN_DEFINITIONS, "recordId", mockTask);

        // verify
        assertEquals(retMap, expectedMap);
    }
}
