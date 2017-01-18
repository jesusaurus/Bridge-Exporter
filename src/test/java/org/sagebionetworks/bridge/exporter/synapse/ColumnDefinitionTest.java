package org.sagebionetworks.bridge.exporter.synapse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Serialize and De-serialize test to/from json for ColumnDefinition object
 */
public class ColumnDefinitionTest {
    private static final String TEST_NAME = "healthCode";
    private static final ColumnType TEST_COLUMN_TYPE = ColumnType.STRING;
    private static final Long TEST_MAXIMUM_SIZE = 36L;
    private static final TransferMethod TEST_TRANSFER_METHOD = TransferMethod.STRING;
    private static final String TEST_DDB_NAME = "healthCode";
    private static final boolean TEST_SANITIZE = true;

    private static final String TEST_JSON = "{\n" +
            "   \"name\":\"healthCode\",\n" +
            "   \"maximumSize\":\"36\",\n" +
            "   \"transferMethod\":\"STRING\",\n" +
            "   \"ddbName\":\"healthCode\",\n" +
            "   \"sanitize\":true\n" +
            "}";

    @Test
    public void testDeserialize () throws IOException {
        final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;

        ColumnDefinition value = mapper.readValue(TEST_JSON, ColumnDefinition.class);

        assertEquals(value.getName(), TEST_NAME);
        assertEquals(value.getDdbName(), TEST_DDB_NAME);
        assertEquals(value.getTransferMethod().getColumnType(), TEST_COLUMN_TYPE);
        assertEquals(value.getMaximumSize(), TEST_MAXIMUM_SIZE);
        assertEquals(value.getSanitize(), TEST_SANITIZE);
        assertEquals(value.getTransferMethod(), TEST_TRANSFER_METHOD);
    }

    @Test
    public void testSerialize () throws IOException {
        ColumnDefinition value = new ColumnDefinition();
        value.setName(TEST_NAME);
        value.setDdbName(TEST_DDB_NAME);
        value.setSanitize(TEST_SANITIZE);
        value.setMaximumSize(TEST_MAXIMUM_SIZE);
        value.setTransferMethod(TEST_TRANSFER_METHOD);

        final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;

        String json = mapper.writeValueAsString(value);
        JsonNode node = mapper.readTree(json);

        assertEquals(node.get("name").asText(), TEST_NAME);
        assertEquals(node.get("ddbName").asText(), TEST_DDB_NAME);
        assertEquals(node.get("maximumSize").asText(), TEST_MAXIMUM_SIZE.toString());
        assertEquals(node.get("sanitize").asText(), String.valueOf(TEST_SANITIZE));
        assertEquals(node.get("transferMethod").asText(), TEST_TRANSFER_METHOD.toString());

    }
}
