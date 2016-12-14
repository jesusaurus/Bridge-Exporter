package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.s3.S3Helper;

public class ExportHelperConvertSurveyTest {
    private static final String DUMMY_ANSWERS_ATTACHMENT_ID = "answers-attachment-id";
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String TEST_SURVEY_ID = "test-survey";
    private static final String DUMMY_RECORD_JSON_TEXT = "{\n" +
            "   \"item\":\"" + TEST_SURVEY_ID + "\",\n" +
            "   \"answers\":\"" + DUMMY_ANSWERS_ATTACHMENT_ID + "\"\n" +
            "}";

    @DataProvider(name = "noAnswersDataProvider")
    public Object[][] noAnswersDataProvider() {
        // "item" omitted to make the tests shorter. It's not used for this particular test anyway.
        return new Object[][] {
                // no answers link
                { "{}" },

                // null answers link
                { "{\"answers\":null}" },

                // answers link not string
                { "{\"answers\":42}" },

                // answers link empty string
                { "{\"answers\":\"\"}" },
        };
    }

    @Test(dataProvider = "noAnswersDataProvider", expectedExceptions = BridgeExporterException.class)
    public void noAnswersLink(String oldJsonText) throws Exception {
        JsonNode oldJsonNode = DefaultObjectMapper.INSTANCE.readTree(oldJsonText);
        new ExportHelper().convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID, oldJsonNode,
                BridgeHelperTest.TEST_SCHEMA);
    }

    @DataProvider(name = "badAnswersAttachmentProvider")
    public Object[][] badAnswersAttachmentProvider() {
        return new Object[][] {
                // malformed answer array
                { "this is malformed JSON" },

                // answer array empty string
                { "" },

                // answer array JSON null
                { "null" },
        };
    }

    @Test(dataProvider = "badAnswersAttachmentProvider", expectedExceptions = BridgeExporterException.class)
    public void badAnswersAttachment(String answersAttachmentText) throws Exception {
        ExportHelper helper = setupHelperWithAnswers(answersAttachmentText);
        helper.convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID, dummyRecordJsonNode(),
                BridgeHelperTest.TEST_SCHEMA);
    }

    @Test
    public void emptyAnswerArray() throws Exception {
        // extreme case where the survey is completely empty
        ExportHelper helper = setupHelperWithAnswers("[]");
        JsonNode convertedSurveyNode = helper.convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID,
                dummyRecordJsonNode(), BridgeHelperTest.TEST_SCHEMA);
        assertTrue(convertedSurveyNode.isObject());
        assertEquals(convertedSurveyNode.size(), 0);
    }

    @Test
    public void normalCase() throws Exception {
        // Make schema with all fields from the below answers.
        UploadSchema testSchema = BridgeHelperTest.simpleSchemaBuilder().fieldDefinitions(ImmutableList.of(
                new UploadFieldDefinition().name("no-question-type-name").type(UploadFieldType.STRING)
                        ,
                new UploadFieldDefinition().name("fallback-to-question-type")
                        .type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("bad-type").type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("null-value").type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("inline-string").type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("attachment-string")
                        .type(UploadFieldType.ATTACHMENT_JSON_BLOB),
                new UploadFieldDefinition().name("integer").type(UploadFieldType.INT),
                new UploadFieldDefinition().name("integer_unit").type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("single-choice")
                        .type(UploadFieldType.INLINE_JSON_BLOB),
                new UploadFieldDefinition().name("inline-multiple-choice")
                        .type(UploadFieldType.INLINE_JSON_BLOB),
                new UploadFieldDefinition().name("attachment-multiple-choice")
                        .type(UploadFieldType.ATTACHMENT_JSON_BLOB)));

        // Make answers array. This is semi-realistic.
        String answersAttachmentText = "[\n" +
                // answer no item
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":\"can't be parsed\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Text\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // answer no question type name
                "   {\n" +
                "       \"textAnswer\":\"can't be parsed\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"item\":\"no-question-type-name\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // answer fall back to question type
                "   {\n" +
                "       \"questionType\":\"Text\",\n" +
                "       \"textAnswer\":\"dummy fallback to question type content\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"item\":\"fallback-to-question-type\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // answer question type name not recognized
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":\"can't be parsed\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"BadType\",\n" +
                "       \"item\":\"bad-type\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // answer null value
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":null,\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Text\",\n" +
                "       \"item\":\"null-value\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // answer not found in schema
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":\"won't show up\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Text\",\n" +
                "       \"item\":\"not-in-schema\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // inline string answer
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":\"dummy inline string content\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Text\",\n" +
                "       \"item\":\"inline-string\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // attachment string answer
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"textAnswer\":\"dummy attachment string content\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Text\",\n" +
                "       \"item\":\"attachment-string\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // integer answer with unit
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"numericAnswer\":42,\n" +
                "       \"unit\":\"barrels\",\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"Integer\",\n" +
                "       \"item\":\"integer\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // single choice
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"choiceAnswers\":[\"single choice answer\"],\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"SingleChoice\",\n" +
                "       \"item\":\"single-choice\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // inline multiple choice
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"choiceAnswers\":[\"inline\", \"multiple\", \"choice\"],\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"MultipleChoice\",\n" +
                "       \"item\":\"inline-multiple-choice\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   },\n" +

                // attachment multiple choice
                "   {\n" +
                "       \"questionType\":0,\n" +
                "       \"choiceAnswers\":[\"attachment\", \"multiple\", \"choice\"],\n" +
                "       \"startDate\":\"2015-11-04T15:11:23-08:00\",\n" +
                "       \"questionTypeName\":\"MultipleChoice\",\n" +
                "       \"item\":\"attachment-multiple-choice\",\n" +
                "       \"endDate\":\"2015-11-04T15:15:47-08:00\"\n" +
                "   }\n" +
                "]";

        // set up export helper - Spy uploadFreeformTextAsAttachment(). This is tested in another test, so just mock it
        ExportHelper helper = spy(setupHelperWithAnswers(answersAttachmentText));
        doAnswer(invocation -> {
            String attachmentText = invocation.getArgumentAt(1, String.class);
            JsonNode attachmentNode = DefaultObjectMapper.INSTANCE.readTree(attachmentText);

            if (attachmentNode.isTextual()) {
                assertEquals(attachmentNode.textValue(), "dummy attachment string content");
                return "attachment-string-attachment-id";
            } else if (attachmentNode.isArray()) {
                assertEquals(attachmentNode.size(), 3);
                assertEquals(attachmentNode.get(0).textValue(), "attachment");
                assertEquals(attachmentNode.get(1).textValue(), "multiple");
                assertEquals(attachmentNode.get(2).textValue(), "choice");
                return "attachment-multiple-choice-attachment-id";
            } else {
                fail("Unexpected node was uploaded");

                // Java doesn't know that fail always throws.
                return null;
            }
        }).when(helper).uploadFreeformTextAsAttachment(eq(DUMMY_RECORD_ID), anyString());

        // execute and validate
        JsonNode convertedSurveyNode = helper.convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID,
                dummyRecordJsonNode(), testSchema);

        assertEquals(convertedSurveyNode.size(), 8);
        assertEquals(convertedSurveyNode.get("fallback-to-question-type").textValue(),
                "dummy fallback to question type content");
        assertEquals(convertedSurveyNode.get("inline-string").textValue(), "dummy inline string content");
        assertEquals(convertedSurveyNode.get("attachment-string").textValue(), "attachment-string-attachment-id");
        assertEquals(convertedSurveyNode.get("integer").intValue(), 42);
        assertEquals(convertedSurveyNode.get("integer_unit").textValue(), "barrels");
        assertEquals(convertedSurveyNode.get("attachment-multiple-choice").textValue(),
                "attachment-multiple-choice-attachment-id");

        JsonNode singleChoiceNode = convertedSurveyNode.get("single-choice");
        assertEquals(singleChoiceNode.size(), 1);
        assertEquals(singleChoiceNode.get(0).textValue(), "single choice answer");

        JsonNode multipleChoiceNode = convertedSurveyNode.get("inline-multiple-choice");
        assertEquals(multipleChoiceNode.size(), 3);
        assertEquals(multipleChoiceNode.get(0).textValue(), "inline");
        assertEquals(multipleChoiceNode.get(1).textValue(), "multiple");
        assertEquals(multipleChoiceNode.get(2).textValue(), "choice");
    }

    private static JsonNode dummyRecordJsonNode() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(DUMMY_RECORD_JSON_TEXT);
    }

    private static ExportHelper setupHelperWithAnswers(String answersAttachmentText) throws Exception {
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // mock S3 Helper
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsString(DUMMY_ATTACHMENT_BUCKET, DUMMY_ANSWERS_ATTACHMENT_ID))
                .thenReturn(answersAttachmentText);

        // set up Export Helper
        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setS3Helper(mockS3Helper);
        return helper;
    }
}
