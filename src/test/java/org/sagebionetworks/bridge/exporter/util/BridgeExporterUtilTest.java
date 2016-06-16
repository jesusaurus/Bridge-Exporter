package org.sagebionetworks.bridge.exporter.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

public class BridgeExporterUtilTest {
    @Test
    public void getFieldDefMapFromSchema() {
        // set up test schema
        UploadFieldDefinition fooField = new UploadFieldDefinition.Builder().withName("foo")
                .withType(UploadFieldType.STRING).build();
        UploadFieldDefinition barField = new UploadFieldDefinition.Builder().withName("bar")
                .withType(UploadFieldType.INT).build();
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().withFieldDefinitions(fooField, barField).build();

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
        UploadFieldDefinition fooField = new UploadFieldDefinition.Builder().withName("foo")
                .withType(UploadFieldType.STRING).build();
        UploadSchema schema = BridgeHelperTest.simpleSchemaBuilder().withStudyId("test-study")
                .withSchemaId("test-schema").withRevision(3).withFieldDefinitions(fooField).build();

        // execute and validate
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyFromSchema(schema);
        assertEquals(schemaKey.getStudyId(), "test-study");
        assertEquals(schemaKey.getSchemaId(), "test-schema");
        assertEquals(schemaKey.getRevision(), 3);
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
}
