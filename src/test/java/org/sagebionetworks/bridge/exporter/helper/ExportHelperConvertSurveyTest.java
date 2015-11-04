package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class ExportHelperConvertSurveyTest {
    private static final String DUMMY_ANSWERS_ATTACHMENT_ID = "answers-attachment-id";
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String TEST_SURVEY_ID = "test-survey";
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId("test-study")
            .withSchemaId(TEST_SURVEY_ID).withRevision(1).build();
    private static final UploadSchema MINIMAL_SCHEMA = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY)
            .addField("foo", "STRING").build();

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
        new ExportHelper().convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID, oldJsonNode, MINIMAL_SCHEMA);
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
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // mock S3 Helper
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsString(DUMMY_ATTACHMENT_BUCKET, DUMMY_ANSWERS_ATTACHMENT_ID))
                .thenReturn(answersAttachmentText);

        // set up Export Helper
        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setS3Helper(mockS3Helper);

        // set up old JSON
        String oldJsonText = "{\n" +
                "   \"item\":\"" + TEST_SURVEY_ID + "\",\n" +
                "   \"answers\":\"" + DUMMY_ANSWERS_ATTACHMENT_ID + "\"\n" +
                "}";
        JsonNode oldJsonNode = DefaultObjectMapper.INSTANCE.readTree(oldJsonText);

        // execute - This throws
        helper.convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID, oldJsonNode, MINIMAL_SCHEMA);
    }

    // TODO refactor these to share test stuff
    @Test
    public void emptyAnswerArray() throws Exception {
        // extreme case where the survey is completely empty

        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // mock S3 Helper
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsString(DUMMY_ATTACHMENT_BUCKET, DUMMY_ANSWERS_ATTACHMENT_ID))
                .thenReturn("[]");

        // set up Export Helper
        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setS3Helper(mockS3Helper);

        // set up old JSON
        String oldJsonText = "{\n" +
                "   \"item\":\"" + TEST_SURVEY_ID + "\",\n" +
                "   \"answers\":\"" + DUMMY_ANSWERS_ATTACHMENT_ID + "\"\n" +
                "}";
        JsonNode oldJsonNode = DefaultObjectMapper.INSTANCE.readTree(oldJsonText);

        // execute - This throws
        helper.convertSurveyRecordToHealthDataJsonNode(DUMMY_RECORD_ID, oldJsonNode, MINIMAL_SCHEMA);
    }

    // answer no item
    // answer no question type name
    // answer fall back to question type
    // answer question type name not recognized
    // answer null value
    // inline string answer
    // attachment string answer
    // integer answer with unit
    // single choice
    // inline multiple choice
    // attachment multiple choice
}
