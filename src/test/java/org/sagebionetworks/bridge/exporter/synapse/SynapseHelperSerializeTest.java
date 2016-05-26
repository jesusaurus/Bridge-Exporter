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
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
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
                TEST_FIELD_NAME, UploadFieldType.STRING, null);
        assertNull(retVal);
    }

    @Test
    public void jsonNull() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.STRING, NullNode.instance);
        assertNull(retVal);
    }

    @Test
    public void booleanTrue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.BOOLEAN, BooleanNode.TRUE);
        assertEquals(retVal, "true");
    }

    @Test
    public void booleanFalse() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.BOOLEAN, BooleanNode.FALSE);
        assertEquals(retVal, "false");
    }

    @Test
    public void booleanInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.BOOLEAN, new TextNode("true"));
        assertNull(retVal);
    }

    @Test
    public void calendarDate() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.CALENDAR_DATE, new TextNode("2015-12-01"));
        assertEquals(retVal, "2015-12-01");
    }

    @Test
    public void calendarDateMalformatted() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.CALENDAR_DATE, new TextNode("foobarbaz"));
        assertNull(retVal);
    }

    @Test
    public void floatValue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.FLOAT, new DecimalNode(new BigDecimal("3.14")));
        assertEquals(retVal, "3.14");
    }

    @Test
    public void floatFromInt() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.FLOAT, new IntNode(42));
        assertEquals(retVal, "42");
    }

    @Test
    public void floatInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.FLOAT, new TextNode("3.14"));
        assertNull(retVal);
    }

    @Test
    public void inlineJsonBlob() throws Exception {
        // based on real JSON blobs
        String jsonText = "[1, 3, 5, 7]";
        JsonNode originalNode = DefaultObjectMapper.INSTANCE.readTree(jsonText);

        // serialize, which basically just copies the JSON text as is
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.INLINE_JSON_BLOB, originalNode);

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
                TEST_FIELD_NAME, UploadFieldType.INT, new IntNode(42));
        assertEquals(retVal, "42");
    }

    @Test
    public void intFromFloat() throws Exception {
        // This simply calls longValue() on the node, which causes truncation instead of rounding.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.INT, new DecimalNode(new BigDecimal("-13.9")));
        assertEquals(retVal, "-13");
    }

    @Test
    public void intInvalidType() throws Exception {
        // We don't parse JSON strings. We only accept JSON booleans.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.INT, new TextNode("-13"));
        assertNull(retVal);
    }

    @Test
    public void stringValue() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.STRING, new TextNode("foobarbaz"));
        assertEquals(retVal, "foobarbaz");
    }

    @Test
    public void timestampString() throws Exception {
        String timestampString = "2015-12-02T12:15-0800";
        long timestampMillis = DateTime.parse(timestampString).getMillis();

        // Synapse timestamps are always epoch milliseconds (long)
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.TIMESTAMP, new TextNode(timestampString));
        assertEquals(retVal, String.valueOf(timestampMillis));
    }

    @Test
    public void timestampInvalidString() throws Exception {
        // We parse strings as ISO timestamps, not as longs.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.TIMESTAMP, new TextNode("1234567890"));
        assertNull(retVal);
    }

    @Test
    public void timestampLong() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.TIMESTAMP, new LongNode(1234567890L));
        assertEquals(retVal, "1234567890");
    }

    @Test
    public void timestampInvalidType() throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.TIMESTAMP, BooleanNode.TRUE);
        assertNull(retVal);
    }

    @Test
    public void attachmentInvalidType() throws Exception {
        // Attachments are strings, which is the attachment ID.
        String retVal = new SynapseHelper().serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.ATTACHMENT_BLOB, new LongNode(1234567890L));
        assertNull(retVal);
    }

    @Test
    public void attachment() throws Exception {
        // Spy uploadFromS3ToSynapseFileHandle(). This has some complex logic that is tested elsewhere. For simplicity
        // of tests, just mock it out.
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        doReturn("dummy-filehandle-id").when(synapseHelper).uploadFromS3ToSynapseFileHandle(MOCK_TEMP_DIR,
                TEST_PROJECT_ID, TEST_FIELD_NAME, UploadFieldType.ATTACHMENT_BLOB, "dummy-attachment-id");

        String retVal = synapseHelper.serializeToSynapseType(MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                TEST_FIELD_NAME, UploadFieldType.ATTACHMENT_BLOB, new TextNode("dummy-attachment-id"));
        assertEquals(retVal, "dummy-filehandle-id");
    }
}
