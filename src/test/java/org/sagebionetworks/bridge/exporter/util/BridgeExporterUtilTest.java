package org.sagebionetworks.bridge.exporter.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
        assertNotNull(schemaKey);
        assertEquals(schemaKey.toString(), "test-study-test-schema-v13");
    }

    @Test
    public void getSchemaKeyForRecord_NoSchemaId() {
        // mock DDB Health Record
        Item recordItem = new Item().withString("studyId", "test-study")
                .withInt("schemaRevision", 13);
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyForRecord(recordItem);
        assertNull(schemaKey);
    }

    // For branch coverage, test missing schema ID and missing schema rev separately.
    @Test
    public void getSchemaKeyForRecord_NoSchemaRev() {
        // mock DDB Health Record
        Item recordItem = new Item().withString("studyId", "test-study")
                .withString("schemaId", "test-schema");
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyForRecord(recordItem);
        assertNull(schemaKey);
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
        Item item = new Item().withString("key", "<p>123 456</p>");
        String out = BridgeExporterUtil.sanitizeDdbValue(item, "key", 5, "dummy-record");
        assertEquals(out, "123 4");
    }

    @Test
    public void sanitizeDdbValueNoMaxLength() {
        Item item = new Item().withString("key", "1234567890");
        String out = BridgeExporterUtil.sanitizeDdbValue(item, "key", null, "dummy-record");
        assertEquals(out, "1234567890");
    }

    @Test
    public void sanitizeJsonValueNotObject() throws Exception {
        String jsonText = "\"not an object\"";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record",
                "dummy-study"));
    }

    @Test
    public void sanitizeJsonValueNoValue() throws Exception {
        String jsonText = "{}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record",
                "dummy-study"));
    }

    @Test
    public void sanitizeJsonValueNullValue() throws Exception {
        String jsonText = "{\"key\":null}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record",
                "dummy-study"));
    }

    @Test
    public void sanitizeJsonValueNotString() throws Exception {
        String jsonText = "{\"key\":42}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        assertNull(BridgeExporterUtil.sanitizeJsonValue(node, "key", 100, "dummy-record",
                "dummy-study"));
    }

    @Test
    public void sanitizeJsonValueNormalCase() throws Exception {
        String jsonText = "{\"key\":\"<p>123 456</p>\"}";
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(jsonText);
        String out = BridgeExporterUtil.sanitizeJsonValue(node, "key", 5, "dummy-record",
                "dummy-study");
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
                { "quote\"quote", 100, "quote\"quote" },
                { "escaped\\\"quote", 100, "escaped\\\"quote" },
                { "[ \"inline\", \"json\", \"blob\" ]", 100, "[ \"inline\", \"json\", \"blob\" ]" },
                { "1234567890", 4, "1234" },
                { "stuff", null, "stuff" },
        };
    }

    @Test(dataProvider = "sanitizeStringDataProvider")
    public void sanitizeString(String in, Integer maxLength, String expected) {
        assertEquals(BridgeExporterUtil.sanitizeString(in, "key", maxLength, "dummy-record",
                "dummy-study"), expected);
    }

    @Test
    public void canConvertToColumnList() {
        // Make column definitions, one for each type.
        ColumnDefinition stringDef = new ColumnDefinition();
        stringDef.setName("my-string");
        stringDef.setTransferMethod(TransferMethod.STRING);
        stringDef.setMaximumSize(42);

        ColumnDefinition stringSetDef = new ColumnDefinition();
        stringSetDef.setName("my-string-set");
        stringSetDef.setTransferMethod(TransferMethod.STRINGSET);
        stringSetDef.setMaximumSize(128);

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
        assertEquals(columnModelList.get(0).getMaximumSize().intValue(), 42);

        assertEquals(columnModelList.get(1).getName(), "my-string-set");
        assertEquals(columnModelList.get(1).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(1).getMaximumSize().intValue(), 128);

        assertEquals(columnModelList.get(2).getName(), "my-date");
        assertEquals(columnModelList.get(2).getColumnType(), ColumnType.DATE);
        assertNull(columnModelList.get(2).getMaximumSize());

        assertEquals(columnModelList.get(3).getName(), "my-large-text");
        assertEquals(columnModelList.get(3).getColumnType(), ColumnType.LARGETEXT);
        assertNull(columnModelList.get(3).getMaximumSize());
    }

    @Test
    public void canGetRowValuesFromRecordBasedOnColumnDefinition() {
        // Make column definitions for test.
        ColumnDefinition stringDef = new ColumnDefinition();
        stringDef.setName("my-string");
        stringDef.setTransferMethod(TransferMethod.STRING);
        stringDef.setMaximumSize(42);

        ColumnDefinition stringSetDef = new ColumnDefinition();
        stringSetDef.setName("my-string-set");
        stringSetDef.setTransferMethod(TransferMethod.STRINGSET);
        stringSetDef.setMaximumSize(128);

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
        renamedColumnDef.setMaximumSize(24);

        ColumnDefinition sanitizeMeDef = new ColumnDefinition();
        sanitizeMeDef.setName("sanitize-me");
        sanitizeMeDef.setTransferMethod(TransferMethod.STRING);
        sanitizeMeDef.setMaximumSize(24);
        sanitizeMeDef.setSanitize(true);

        ColumnDefinition sanitizedLargeTextDef = new ColumnDefinition();
        sanitizedLargeTextDef.setName("sanitized-large-text");
        sanitizedLargeTextDef.setTransferMethod(TransferMethod.LARGETEXT);
        sanitizedLargeTextDef.setSanitize(true);

        ColumnDefinition truncateMeDef = new ColumnDefinition();
        truncateMeDef.setName("truncate-me");
        truncateMeDef.setTransferMethod(TransferMethod.STRING);
        truncateMeDef.setMaximumSize(3);
        truncateMeDef.setSanitize(true);

        List<ColumnDefinition> columnDefinitionList = ImmutableList.of(stringDef, stringSetDef, dateDef, largeTextDef,
                renamedColumnDef, sanitizeMeDef, sanitizedLargeTextDef, truncateMeDef);

        // Set up DDB record for test.
        Item ddbRecord = new Item()
                .withString("my-string", "my-string-value")
                .withStringSet("my-string-set", "val1", "val2")
                .withLong("my-date", 1234567890)
                .withString("my-large-text", "my-large-text-value")
                .withString("ddb-column", "ddb-column-value")
                .withString("sanitize-me", "<b><i><u>Sanitize me!</b></i></u>")
                .withString("sanitized-large-text", "<b>formatted string</b>")
                .withString("truncate-me", "truncate-me-value");

        // execute and validate
        Map<String, String> rowMap = new HashMap<>();
        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(rowMap, ddbRecord, columnDefinitionList,
                "record-id");
        assertEquals(rowMap.size(), 8);
        assertEquals("my-string-value", rowMap.get("my-string"));
        assertEquals("val1,val2", rowMap.get("my-string-set"));
        assertEquals("1234567890", rowMap.get("my-date"));
        assertEquals("my-large-text-value", rowMap.get("my-large-text"));
        assertEquals("ddb-column-value", rowMap.get("renamed-column"));
        assertEquals("Sanitize me!", rowMap.get("sanitize-me"));
        assertEquals("formatted string", rowMap.get("sanitized-large-text"));
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
        testDefinition1.setMaximumSize(36);
        testDefinition1.setTransferMethod(TransferMethod.STRING);
        columnDefinitionBuilder.add(testDefinition1);

        testColumnDefinitions = columnDefinitionBuilder.build();

        // process
        Map<String, String> retMap = new HashMap<>();
        BridgeExporterUtil.getRowValuesFromRecordBasedOnColumnDefinition(retMap, testRecord, testColumnDefinitions, "recordId");

        // verify
        assertEquals(retMap, expectedMap);
    }
    
    @Test
    public void serializeSubstudyMemberships() {
        Map<String,String> map = new HashMap<>();
        map.put("subA", "<none>"); // so DDB serialization doesn't drop the entry, use "<none>" as missing key
        map.put("subB", "extB");
        map.put("subC", "extC");
        map.put("subD", ""); // this works, though we don't persist this.
        
        String output = BridgeExporterUtil.serializeSubstudyMemberships(map);
        assertEquals("|subA=|subB=extB|subC=extC|subD=|", output);
    }    
    
    @Test
    public void serializeSubstudyMembershipsOneEntry() {
        Map<String,String> map = new HashMap<>();
        map.put("subB", "extB");
        
        String output = BridgeExporterUtil.serializeSubstudyMemberships(map);
        assertEquals("|subB=extB|", output);
    }
    
    @Test
    public void serializeSubstudyMembershipsNull() {
        assertNull(BridgeExporterUtil.serializeSubstudyMemberships(null));
    }
    
    @Test
    public void serializeSubstudyMembershipsBlank() {
        assertNull(BridgeExporterUtil.serializeSubstudyMemberships(ImmutableMap.of()));
    }
    
}
