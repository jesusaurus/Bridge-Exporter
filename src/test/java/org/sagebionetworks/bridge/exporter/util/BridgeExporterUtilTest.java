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
import org.testng.annotations.DataProvider;
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

    @DataProvider(name = "sanitizeStringDataProvider")
    public Object[][] sanitizeStringDataProvider() {
        return new Object[][] {
                { null, 100, null },
                { "", 100, "" },
                { "lorem ipsum", 100, "lorem ipsum" },
                { "<b>bold text</b>", 100, "bold text" },
                { "imbalanced</i> <p>tags", 100, "imbalanced tags" },
                { "newlines\n\n\nCRLF\r\ntabs\t\ttabs", 1000, "newlines CRLF tabs tabs" },
                { "quote\"quote", 100, "quote\\\"quote" },
                { "escaped\\\"quote", 100, "escaped\\\\\\\"quote" },
                { "[ \"inline\", \"json\", \"blob\" ]", 100, "[ \\\"inline\\\", \\\"json\\\", \\\"blob\\\" ]" },
                { "1234567890", 4, "1234" },
                { "stuff", null, "stuff" },
        };
    }

    @Test(dataProvider = "sanitizeStringDataProvider")
    public void sanitizeString(String in, Integer maxLength, String expected) {
        assertEquals(BridgeExporterUtil.sanitizeString(in, "key", maxLength, "dummy-record"), expected);
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
        // Make column definitions, one for each type.
        ColumnDefinition stringDef = new ColumnDefinition();
        stringDef.setName("my-string");
        stringDef.setTransferMethod(TransferMethod.STRING);
        stringDef.setMaximumSize(42L);

        ColumnDefinition stringSetDef = new ColumnDefinition();
        stringSetDef.setName("my-string-set");
        stringSetDef.setTransferMethod(TransferMethod.STRINGSET);
        stringSetDef.setMaximumSize(128L);

        ColumnDefinition dateDef = new ColumnDefinition();
        dateDef.setName("my-date");
        dateDef.setTransferMethod(TransferMethod.DATE);

        ColumnDefinition largeTextDef = new ColumnDefinition();
        largeTextDef.setName("my-large-text");
        largeTextDef.setTransferMethod(TransferMethod.LARGETEXT);

        List<ColumnDefinition> columnDefinitionList = ImmutableList.of(stringDef, stringSetDef, dateDef, largeTextDef);

        // execute and validate
        List<ColumnModel> columnModelList = BridgeExporterUtil.convertToColumnList(columnDefinitionList);
        assertEquals(columnModelList.size(), 4);

        assertEquals(columnModelList.get(0).getName(), "my-string");
        assertEquals(columnModelList.get(0).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(0).getMaximumSize().longValue(), 42);

        assertEquals(columnModelList.get(1).getName(), "my-string-set");
        assertEquals(columnModelList.get(1).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(1).getMaximumSize().longValue(), 128);

        assertEquals(columnModelList.get(2).getName(), "my-date");
        assertEquals(columnModelList.get(2).getColumnType(), ColumnType.DATE);

        assertEquals(columnModelList.get(3).getName(), "my-large-text");
        assertEquals(columnModelList.get(3).getColumnType(), ColumnType.LARGETEXT);
    }

    @Test
    public void canGetRowValuesFromRecordBasedOnColumnDefinition() {
        // Make column definitions for test.
        ColumnDefinition stringDef = new ColumnDefinition();
        stringDef.setName("my-string");
        stringDef.setTransferMethod(TransferMethod.STRING);
        stringDef.setMaximumSize(42L);

        ColumnDefinition stringSetDef = new ColumnDefinition();
        stringSetDef.setName("my-string-set");
        stringSetDef.setTransferMethod(TransferMethod.STRINGSET);
        stringSetDef.setMaximumSize(128L);

        ColumnDefinition dateDef = new ColumnDefinition();
        dateDef.setName("my-date");
        dateDef.setTransferMethod(TransferMethod.DATE);

        ColumnDefinition largeTextDef = new ColumnDefinition();
        largeTextDef.setName("my-large-text");
        largeTextDef.setTransferMethod(TransferMethod.LARGETEXT);

        ColumnDefinition renamedColumnDef = new ColumnDefinition();
        renamedColumnDef.setName("renamed-column");
        renamedColumnDef.setDdbName("ddb-column");
        renamedColumnDef.setTransferMethod(TransferMethod.STRING);
        renamedColumnDef.setMaximumSize(24L);

        ColumnDefinition sanitizeMeDef = new ColumnDefinition();
        sanitizeMeDef.setName("sanitize-me");
        sanitizeMeDef.setTransferMethod(TransferMethod.STRING);
        sanitizeMeDef.setMaximumSize(24L);
        sanitizeMeDef.setSanitize(true);

        ColumnDefinition truncateMeDef = new ColumnDefinition();
        truncateMeDef.setName("truncate-me");
        truncateMeDef.setTransferMethod(TransferMethod.STRING);
        truncateMeDef.setMaximumSize(3L);
        truncateMeDef.setSanitize(true);

        List<ColumnDefinition> columnDefinitionList = ImmutableList.of(stringDef, stringSetDef, dateDef, largeTextDef,
                renamedColumnDef, sanitizeMeDef, truncateMeDef);

        // Set up DDB record for test.
        Item ddbRecord = new Item()
                .withString("my-string", "my-string-value")
                .withStringSet("my-string-set", "val1", "val2")
                .withLong("my-date", 1234567890)
                .withString("my-large-text", "my-large-text-value")
                .withString("ddb-column", "ddb-column-value")
                .withString("sanitize-me", "<b><i><u>Sanitize me!</b></i></u>")
                .withString("truncate-me", "truncate-me-value");

        // execute and validate
        Map<String, String> rowMap = new HashMap<>();
        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(rowMap, ddbRecord, columnDefinitionList,
                "record-id");
        assertEquals(rowMap.size(), 7);
        assertEquals("my-string-value", rowMap.get("my-string"));
        assertEquals("val1,val2", rowMap.get("my-string-set"));
        assertEquals("1234567890", rowMap.get("my-date"));
        assertEquals("my-large-text-value", rowMap.get("my-large-text"));
        assertEquals("ddb-column-value", rowMap.get("renamed-column"));
        assertEquals("Sanitize me!", rowMap.get("sanitize-me"));
        assertEquals("tru", rowMap.get("truncate-me"));
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
