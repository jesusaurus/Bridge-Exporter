package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
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
import com.google.common.collect.Multiset;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;

// Tests for SynapseHelper.serializeToSynapseType()
public class SynapseHelperSerializeTest {
    private static final File MOCK_TEMP_DIR = mock(File.class);
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String TEST_RECORD_ID = "test-record-id";
    private static final String TEST_FIELD_NAME = "test-field-name";

    @DataProvider(name = "testSerializeProvider")
    public Object[][] testSerializeProvider() {
        // fieldType, input, expected
        return new Object[][] {
                { UploadFieldType.STRING, null, null },
                { UploadFieldType.STRING, NullNode.instance, null },

                { UploadFieldType.BOOLEAN, BooleanNode.TRUE, "true" },
                { UploadFieldType.BOOLEAN, BooleanNode.FALSE, "false" },
                // We don't parse JSON strings. We only accept JSON booleans.
                { UploadFieldType.BOOLEAN, new TextNode("true"), null },

                { UploadFieldType.CALENDAR_DATE, new TextNode("2015-12-01"), "2015-12-01" },
                { UploadFieldType.DURATION_V2, new TextNode("PT1H"), "PT1H" },
                { UploadFieldType.TIME_V2, new TextNode("13:07:56.123"), "13:07:56.123" },

                { UploadFieldType.FLOAT, new DecimalNode(new BigDecimal("3.14")), "3.14" },
                { UploadFieldType.FLOAT, new IntNode(42), "42" },
                // We don't parse JSON strings. We only accept numeric JSON values.
                { UploadFieldType.FLOAT, new TextNode("3.14"), null },

                { UploadFieldType.INT, new IntNode(42), "42" },
                // This simply calls longValue() on the node, which causes truncation instead of rounding.
                { UploadFieldType.INT, new DecimalNode(new BigDecimal("-13.9")), "-13" },
                // We don't parse JSON strings. We only accept numeric JSON values.
                { UploadFieldType.INT, new TextNode("-13"), null },

                { UploadFieldType.SINGLE_CHOICE, new TextNode("foobarbaz"), "foobarbaz" },
                { UploadFieldType.STRING, new TextNode("foobarbaz"), "foobarbaz" },

                // Test removing HTML
                { UploadFieldType.SINGLE_CHOICE, new TextNode("<a href=\"sagebase.org\">link</a>"), "link" },
                { UploadFieldType.STRING, new TextNode("<a href=\"sagebase.org\">link</a>"), "link" },

                // These types are not supported by serialize() because they serialize into multiple columns.
                // This test is mainly for branch coverage. This code path should never be hit in real life.
                { UploadFieldType.MULTI_CHOICE, new TextNode("value doesn't matter"), null },
                { UploadFieldType.TIMESTAMP, new TextNode("value doesn't matter"), null },
        };
    }

    @Test(dataProvider = "testSerializeProvider")
    public void testSerialize(UploadFieldType fieldType, JsonNode input, String expected) throws Exception {
        testHelper(new Metrics(), fieldDefForType(fieldType), input, expected);
    }

    @Test
    public void inlineJsonBlob() throws Exception {
        // based on real JSON blobs
        String jsonText = "[1, 3, 5, 7]";
        JsonNode originalNode = DefaultObjectMapper.INSTANCE.readTree(jsonText);

        // serialize, which basically just copies the JSON text as is
        String retVal = new SynapseHelper().serializeToSynapseType(new Metrics(), MOCK_TEMP_DIR, TEST_PROJECT_ID,
                TEST_RECORD_ID, fieldDefForType(UploadFieldType.INLINE_JSON_BLOB), originalNode);

        // parse back into JSON and compare
        JsonNode reparsedNode = DefaultObjectMapper.INSTANCE.readTree(retVal);
        assertTrue(reparsedNode.isArray());
        assertEquals(reparsedNode.size(), 4);
        assertEquals(reparsedNode.get(0).intValue(), 1);
        assertEquals(reparsedNode.get(1).intValue(), 3);
        assertEquals(reparsedNode.get(2).intValue(), 5);
        assertEquals(reparsedNode.get(3).intValue(), 7);
    }

    @DataProvider(name = "stringTypeProvider")
    public Object[][] stringTypeProvider() {
        return new Object[][] {
                { UploadFieldType.SINGLE_CHOICE },
                { UploadFieldType.STRING },
        };
    }

    @Test(dataProvider = "stringTypeProvider")
    public void stringSanitized(UploadFieldType stringType) throws Exception {
        // Use an extra short field def.
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name(TEST_FIELD_NAME).type(stringType)
                .maxLength(10);

        // String value has newlines and tabs that need to be stripped out.
        // Newlines turned into spaces, string truncated to length 10
        testHelper(new Metrics(), fieldDef, new TextNode("asdf\njkl;\tlorem ipsum dolor"), "asdf jkl; ");
    }

    // branch coverage
    @Test(dataProvider = "stringTypeProvider")
    public void stringUnboundedTrue(UploadFieldType stringType) throws Exception {
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name(TEST_FIELD_NAME).type(stringType).
                unboundedText(true);
        testHelper(new Metrics(), fieldDef, new TextNode("unbounded text not really"), "unbounded text not really");
    }

    // branch coverage
    @Test(dataProvider = "stringTypeProvider")
    public void stringUnboundedFalse(UploadFieldType stringType) throws Exception {
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name(TEST_FIELD_NAME).type(stringType)
                .unboundedText(false);
        testHelper(new Metrics(), fieldDef, new TextNode("not really unbounded text"), "not really unbounded text");
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
        Metrics metrics = new Metrics();
        testHelper(metrics, fieldDefForType(attachmentType), new LongNode(1234567890L), null);

        // Validate metrics - There were no attachments.
        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("numAttachments"), 0);
    }

    @Test(dataProvider = "attachmentTypeProvider")
    public void attachment(UploadFieldType attachmentType) throws Exception {
        UploadFieldDefinition attachmentFieldDef = fieldDefForType(attachmentType);

        // Spy uploadFromS3ToSynapseFileHandle(). This has some complex logic that is tested elsewhere. For simplicity
        // of tests, just mock it out.
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        doReturn("dummy-filehandle-id").when(synapseHelper).uploadFromS3ToSynapseFileHandle(MOCK_TEMP_DIR,
                TEST_PROJECT_ID, attachmentFieldDef, "dummy-attachment-id");

        // execute
        Metrics metrics = new Metrics();
        String retVal = synapseHelper.serializeToSynapseType(metrics, MOCK_TEMP_DIR, TEST_PROJECT_ID, TEST_RECORD_ID,
                attachmentFieldDef, new TextNode("dummy-attachment-id"));
        assertEquals(retVal, "dummy-filehandle-id");

        // Validate metrics
        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("numAttachments"), 1);
    }

    private static UploadFieldDefinition fieldDefForType(UploadFieldType type) {
        return new UploadFieldDefinition().name(TEST_FIELD_NAME).type(type);
    }

    private static void testHelper(Metrics metrics, UploadFieldDefinition fieldDef, JsonNode input,
            String expected) throws Exception {
        String retVal = new SynapseHelper().serializeToSynapseType(metrics, MOCK_TEMP_DIR, TEST_PROJECT_ID,
                TEST_RECORD_ID, fieldDef, input);
        assertEquals(retVal, expected);
    }
}
