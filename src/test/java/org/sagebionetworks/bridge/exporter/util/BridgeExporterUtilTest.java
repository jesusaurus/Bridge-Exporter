package org.sagebionetworks.bridge.exporter.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.bridge.exporter.synapse.TransferMethod;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class BridgeExporterUtilTest {
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
        assertNull(BridgeExporterUtil.sanitizeString(null, "key", 100, "dummy-record"));
    }

    @Test
    public void sanitizeStringEmpty() {
        String out = BridgeExporterUtil.sanitizeString("", "key", 100, "dummy-record");
        assertEquals(out, "");
    }

    @Test
    public void sanitizeStringPassthrough() {
        String out = BridgeExporterUtil.sanitizeString("lorem ipsum", "key", 100, "dummy-record");
        assertEquals(out, "lorem ipsum");
    }

    @Test
    public void sanitizeStringRemoveHtml() {
        String out = BridgeExporterUtil.sanitizeString("<b>bold text</b>", "key", 100, "dummy-record");
        assertEquals(out, "bold text");
    }

    @Test
    public void sanitizeStringRemovePartialHtml() {
        String out = BridgeExporterUtil.sanitizeString("imbalanced</i> <p>tags", "key", 100, "dummy-record");
        assertEquals(out, "imbalanced tags");
    }

    @Test
    public void sanitizeStringRemoveBadChars() {
        String in = "newlines\n\n\n" +
                "CRLF\r\n" +
                "tabs\t\ttabs";
        String out = BridgeExporterUtil.sanitizeString(in, "key", 1000, "dummy-record");
        assertEquals(out, "newlines CRLF tabs tabs");
    }

    @Test
    public void sanitizeStringTooLong() {
        String out = BridgeExporterUtil.sanitizeString("1234567890", "key", 4, "dummy-record");
        assertEquals(out, "1234");
    }

    // branch coverage
    @Test
    public void sanitizeStringNullMaxLength() {
        String out = BridgeExporterUtil.sanitizeString("stuff", "key", null, "dummy-record");
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
        final String testColumnModelName1 = "test_column_model_1";
        final String testColumnModelName2 = "test_column_model_2";

        List<ColumnModel> testColumnList;

        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel testModel1 = new ColumnModel();
        testModel1.setName(testColumnModelName1);
        testModel1.setColumnType(ColumnType.STRING);
        testModel1.setMaximumSize(36L);
        columnListBuilder.add(testModel1);

        ColumnModel testModel2 = new ColumnModel();
        testModel2.setName(testColumnModelName2);
        testModel2.setColumnType(ColumnType.STRING);
        testModel2.setMaximumSize(48L);
        columnListBuilder.add(testModel2);

        testColumnList = columnListBuilder.build();

        List<ColumnDefinition> testColumnDefinitions;

        ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();

        ColumnDefinition testDefinition1 = new ColumnDefinition();
        testDefinition1.setName(testColumnModelName1);
        testDefinition1.setTransferMethod(TransferMethod.STRING);
        testDefinition1.setMaximumSize(36L);
        columnDefinitionBuilder.add(testDefinition1);

        ColumnDefinition testDefinition2 = new ColumnDefinition();
        testDefinition2.setName(testColumnModelName2);
        testDefinition2.setTransferMethod(TransferMethod.STRING);
        testDefinition2.setMaximumSize(48L);
        columnDefinitionBuilder.add(testDefinition2);

        testColumnDefinitions = columnDefinitionBuilder.build();

        assertEquals(BridgeExporterUtil.convertToColumnList(testColumnDefinitions), testColumnList);
    }

    @Test
    public void canGetRowValuesFromRecordBasedOnColumnDefinition() {
        final String testStringName = "test_string";
        final String testStringSetName = "test_string_set";
        final String testDateName = "test_date";
        final String testSanitize = "test_sanitize";

        // create mock record
        Item testRecord = new Item();
        testRecord.withString(testStringName, "test_string_value");
        testRecord.withStringSet(testStringSetName, "test_string_set_value_1", "test_string_set_value_2");
        testRecord.withLong(testDateName, 1484181511);
        testRecord.with(testSanitize, "imbalanced</i> <p>tags");

        // create expected map
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(testStringName, "test_string_value");
        expectedMap.put(testStringSetName, "test_string_set_value_1,test_string_set_value_2");
        expectedMap.put(testDateName, "1484181511");
        expectedMap.put(testSanitize, "imbalanced tags");

        // create mock column definitions
        List<ColumnDefinition> testColumnDefinitions;

        ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();

        ColumnDefinition testDefinition1 = new ColumnDefinition();
        testDefinition1.setName(testStringName);
        testDefinition1.setMaximumSize(36L);
        testDefinition1.setTransferMethod(TransferMethod.STRING);
        testDefinition1.setDdbName(testStringName);
        columnDefinitionBuilder.add(testDefinition1);

        ColumnDefinition testDefinition2 = new ColumnDefinition();
        testDefinition2.setName(testStringSetName);
        testDefinition2.setTransferMethod(TransferMethod.STRINGSET);
        testDefinition2.setDdbName(testStringSetName);
        testDefinition2.setMaximumSize(100L);
        columnDefinitionBuilder.add(testDefinition2);

        ColumnDefinition testDefinition4 = new ColumnDefinition();
        testDefinition4.setName(testDateName);
        testDefinition4.setTransferMethod(TransferMethod.DATE);
        testDefinition4.setDdbName(testDateName);
        testDefinition4.setMaximumSize(36L);
        columnDefinitionBuilder.add(testDefinition4);

        ColumnDefinition testDefinition5 = new ColumnDefinition();
        testDefinition5.setName(testSanitize);
        testDefinition5.setMaximumSize(36L);
        testDefinition5.setTransferMethod(TransferMethod.STRING);
        testDefinition5.setDdbName(testSanitize);
        testDefinition5.setSanitize(true);
        columnDefinitionBuilder.add(testDefinition5);

        testColumnDefinitions = columnDefinitionBuilder.build();

        // process
        Map<String, String> retMap = new HashMap<>();
        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(retMap, testRecord, testColumnDefinitions, "recordId");

        // verify
        assertEquals(retMap, expectedMap);
    }

    @Test
    public void canGetRowValuesWIthoutDdbName() {
        final String testStringName = "test_string";

        // create mock record
        Item testRecord = new Item();
        testRecord.withString(testStringName, "test_string_value");

        // create expected map
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(testStringName, "test_string_value");

        // create mock column definitions
        List<ColumnDefinition> testColumnDefinitions;

        ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();

        ColumnDefinition testDefinition1 = new ColumnDefinition();
        testDefinition1.setName(testStringName);
        testDefinition1.setMaximumSize(36L);
        testDefinition1.setTransferMethod(TransferMethod.STRING);
        columnDefinitionBuilder.add(testDefinition1);

        testColumnDefinitions = columnDefinitionBuilder.build();

        // process
        Map<String, String> retMap = new HashMap<>();
        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(retMap, testRecord, testColumnDefinitions, "recordId");

        // verify
        assertEquals(retMap, expectedMap);
    }
}
