package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;

// Tests for SynapseHelper.serializeToSynapseType()
public class SynapseHelperSerializeTest {
    private static final File MOCK_TEMP_DIR = mock(File.class);
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String TEST_RECORD_ID = "test-record-id";
    private static final String TEST_FIELD_NAME = "test-field-name";

    @Test
    public void nullValue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.STRING), null);
        assertNull(retVal);
    }

    @Test
    public void jsonNull() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.STRING), NullNode.instance);
        assertNull(retVal);
    }

    @Test
    public void booleanTrue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.BOOLEAN), BooleanNode.TRUE);
        assertEquals(retVal, "true");
    }

    @Test
    public void booleanFalse() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.BOOLEAN), BooleanNode.FALSE);
        assertEquals(retVal, "false");
    }

    @Test
    public void booleanInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.BOOLEAN), new TextNode("true"));
        assertNull(retVal);
    }

    @Test
    public void calendarDate() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.CALENDAR_DATE), new TextNode("2015-12-01"));
        assertEquals(retVal, "2015-12-01");
    }

    @Test
    public void duration() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.DURATION_V2), new TextNode("PT1H"));
        assertEquals(retVal, "PT1H");
    }

    @Test
    public void floatValue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.FLOAT), new DecimalNode(new BigDecimal("3.14")));
        assertEquals(retVal, "3.14");
    }

    @Test
    public void floatFromInt() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.FLOAT), new IntNode(42));
        assertEquals(retVal, "42");
    }

    @Test
    public void floatInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.FLOAT), new TextNode("3.14"));
        assertNull(retVal);
    }

    @Test
    public void inlineJsonBlob() throws Exception {
        // based on real JSON blobs
        String jsonText = "[1, 3, 5, 7]";
        JsonNode originalNode = DefaultObjectMapper.INSTANCE.readTree(jsonText);

        // serialize, which basically just copies the JSON text as is
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.INLINE_JSON_BLOB), originalNode);

        // parse back into JSON and compare
        JsonNode reparsedNode = DefaultObjectMapper.INSTANCE.readTree(retVal);
        assertTrue(reparsedNode.isArray());
        assertEquals(reparsedNode.size(), 4);
        assertEquals(reparsedNode.get(0).intValue(), 1);
        assertEquals(reparsedNode.get(1).intValue(), 3);
        assertEquals(reparsedNode.get(2).intValue(), 5);
        assertEquals(reparsedNode.get(3).intValue(), 7);
    }

    @Test
    public void intValue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.INT), new IntNode(42));
        assertEquals(retVal, "42");
    }

    @Test
    public void intFromFloat() throws Exception {
        // This simply calls longValue() on the node, which causes truncation instead of rounding.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.INT), new DecimalNode(new BigDecimal("-13.9")));
        assertEquals(retVal, "-13");
    }

    @Test
    public void intInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.INT), new TextNode("-13"));
        assertNull(retVal);
    }

    @DataProvider(name = "stringTypeProvider")
    public Object[][] stringTypeProvider() {
        return new Object[][] {
                { UploadFieldType.SINGLE_CHOICE },
                { UploadFieldType.STRING },
        };
    }

    @Test(dataProvider = "stringTypeProvider")
    public void stringValue(UploadFieldType stringType) throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(stringType), new TextNode("foobarbaz"));
        assertEquals(retVal, "foobarbaz");
    }

    @Test(dataProvider = "stringTypeProvider")
    public void stringSanitized(UploadFieldType stringType) throws Exception {
        // Use an extra short field def.
        UploadFieldDefinition fieldDef = new UploadFieldDefinition.Builder().withName(TEST_FIELD_NAME)
                .withType(stringType).withMaxLength(10).build();

        // String value has newlines and tabs that need to be stripped out.
        String input = "asdf\njkl;\tlorem ipsum dolor";
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDef, new TextNode(input));

        // Newlines turned into spaces, string truncated to length 10
        assertEquals(retVal, "asdf jkl; ");
    }

    @Test(dataProvider = "stringTypeProvider")
    public void stringStripHtml(UploadFieldType stringType) throws Exception {
        String input = "<a href=\"sagebase.org\">link</a>";
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(stringType), new TextNode(input));
        assertEquals(retVal, "link");
    }

    @Test
    public void time() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(UploadFieldType.TIME_V2), new TextNode("13:07:56.123"));
        assertEquals(retVal, "13:07:56.123");
    }

    @DataProvider(name = "attachmentTypeProvider")
    public Object[][] attachmentTypeProvider() {
        return new Object[][] {
                { UploadFieldType.ATTACHMENT_BLOB },
                { UploadFieldType.ATTACHMENT_CSV },
                { UploadFieldType.ATTACHMENT_JSON_BLOB },
                { UploadFieldType.ATTACHMENT_JSON_TABLE },
                { UploadFieldType.ATTACHMENT_V2 },
        };
    }

    @Test(dataProvider = "attachmentTypeProvider")
    public void attachmentInvalidType(UploadFieldType attachmentType) throws Exception {
        // Attachments are strings, which is the attachment ID.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(attachmentType), new LongNode(1234567890L));
        assertNull(retVal);
    }

    @Test(dataProvider = "attachmentTypeProvider")
    public void attachment(UploadFieldType attachmentType) throws Exception {
        UploadFieldDefinition attachmentFieldDef = fieldDefForType(attachmentType);

        // Spy uploadFromS3ToSynapseFileHandle(). This has some complex logic that is tested elsewhere. For simplicity
        // of tests, just mock it out.
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        doReturn("dummy-filehandle-id").when(synapseHelper).uploadFromS3ToSynapseFileHandle(MOCK_TEMP_DIR,
                TEST_PROJECT_ID, attachmentFieldDef, "dummy-attachment-id");

        String retVal = synapseHelper.serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                attachmentFieldDef, new TextNode("dummy-attachment-id"));
        assertEquals(retVal, "dummy-filehandle-id");
    }

    // These types are not supported by serialize() because they serialize into multiple columns.
    @DataProvider(name = "unsupportedTypeProvider")
    public Object[][] unsupportedTypeProvider() {
        return new Object[][] {
                { UploadFieldType.MULTI_CHOICE },
                { UploadFieldType.TIMESTAMP },
        };
    }

    // This test is mainly for branch coverage. This code path should never be hit in real life.
    @Test(dataProvider = "unsupportedTypeProvider")
    public void unsupportedType(UploadFieldType unsupportedType) throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                fieldDefForType(unsupportedType), new TextNode("value doesn't matter"));
        assertNull(retVal);
    }

    private static UploadFieldDefinition fieldDefForType(UploadFieldType type) {
        return new UploadFieldDefinition.Builder().withName(TEST_FIELD_NAME).withType(type).build();
    }
}
