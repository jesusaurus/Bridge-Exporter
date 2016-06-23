package org.sagebionetworks.bridge.exporter.handler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;

public class HealthDataExportHandlerTest {
    private static final String FIELD_NAME = "foo-field";
    private static final String FIELD_NAME_TIMEZONE = FIELD_NAME + ".timezone";

    private static final UploadFieldDefinition MULTI_CHOICE_FIELD_DEF = new UploadFieldDefinition.Builder()
            .withName(FIELD_NAME).withType(UploadFieldType.MULTI_CHOICE).withMultiChoiceAnswerList("foo", "bar", "baz",
                    "true", "42").build();
    private static final UploadFieldDefinition OTHER_CHOICE_FIELD_DEF = new UploadFieldDefinition.Builder()
            .withAllowOtherChoices(true).withName(FIELD_NAME).withType(UploadFieldType.MULTI_CHOICE)
            .withMultiChoiceAnswerList("one", "two").build();

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
}
