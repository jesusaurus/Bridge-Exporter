package org.sagebionetworks.bridge.exporter.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class BridgeExporterRequestTest {
    private static final String TEST_DATE_STRING = "2015-11-30";
    private static final LocalDate TEST_DATE = LocalDate.parse(TEST_DATE_STRING);
    private static final String TEST_DDB_PREFIX_OVERRIDE = "test-ddb-prefix-override";
    private static final Map<String, String> TEST_PROJECT_OVERRIDE_MAP = ImmutableMap.of("test-study",
            "test-project-id");
    private static final String TEST_RECORD_OVERRIDE = "test-record-override";
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId("test-study")
            .withSchemaId("test-schema").withRevision(13).build();
    private static final String TEST_TAG = "test-tag";

    @Test
    public void withDate() {
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(TEST_DATE).build();
        assertEquals(request.getDate(), TEST_DATE);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);
    }

    @Test
    public void withRecordOverride() {
        BridgeExporterRequest request = new BridgeExporterRequest.Builder()
                .withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
        assertEquals(request.getRecordIdS3Override(), TEST_RECORD_OVERRIDE);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);
    }

    @Test
    public void withOptionalParams() {
        // Make collections. We make them specifically for this test, because we want to modify them to make sure they
        // we can't backdoor-modify the request.
        Set<String> originalStudyWhitelist = Sets.newHashSet("foo-study", "bar-study");

        Map<String, String> originalProjectOverrideMap = new HashMap<>();
        originalProjectOverrideMap.put("foo-study", "foo-project-id");
        originalProjectOverrideMap.put("bar-study", "bar-project-id");

        UploadSchemaKey fooSchemaKey = new UploadSchemaKey.Builder().withStudyId("foo-study")
                .withSchemaId("foo-schema").withRevision(3).build();
        UploadSchemaKey barSchemaKey = new UploadSchemaKey.Builder().withStudyId("bar-study")
                .withSchemaId("bar-schema").withRevision(7).build();
        Set<UploadSchemaKey> originalTableWhitelist = Sets.newHashSet(fooSchemaKey, barSchemaKey);

        // make request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(TEST_DATE)
                .withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE)
                .withSharingMode(BridgeExporterSharingMode.PUBLIC_ONLY).withStudyWhitelist(originalStudyWhitelist)
                .withSynapseProjectOverrideMap(originalProjectOverrideMap).withTableWhitelist(originalTableWhitelist)
                .withTag(TEST_TAG).build();

        // validate
        assertEquals(request.getDate(), TEST_DATE);
        assertEquals(request.getExporterDdbPrefixOverride(), TEST_DDB_PREFIX_OVERRIDE);
        assertNull(request.getRecordIdS3Override());
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.PUBLIC_ONLY);
        assertEquals(request.getStudyWhitelist(), originalStudyWhitelist);
        assertEquals(request.getSynapseProjectOverrideMap(), originalProjectOverrideMap);
        assertEquals(request.getTableWhitelist(), originalTableWhitelist);
        assertEquals(request.getTag(), TEST_TAG);

        // Validate that changes to the original collections won't be reflected in the request.
        originalStudyWhitelist.add("new-study");
        assertFalse(request.getStudyWhitelist().contains("new-study"));

        originalProjectOverrideMap.put("new-study", "new-project-id");
        assertFalse(request.getSynapseProjectOverrideMap().containsKey("new-study"));

        originalTableWhitelist.add(TEST_SCHEMA_KEY);
        assertFalse(request.getTableWhitelist().contains(TEST_SCHEMA_KEY));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Either date or recordIdS3Override must be specified, but not both.")
    public void withBothDateAndRecordOverride() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Either date or recordIdS3Override must be specified, but not both.")
    public void withNeitherDateNorRecordOverride() {
        new BridgeExporterRequest.Builder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Either date or recordIdS3Override must be specified, but not both.")
    public void emptyRecordOverride() {
        new BridgeExporterRequest.Builder().withRecordIdS3Override("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Either date or recordIdS3Override must be specified, but not both.")
    public void blankRecordOverride() {
        new BridgeExporterRequest.Builder().withRecordIdS3Override("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void ddbPrefixOverrideWithoutProjectOverride() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE)
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void projectOverrideWithoutDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE)
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void emptyDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withExporterDdbPrefixOverride("")
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void blankDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withExporterDdbPrefixOverride("   ")
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If synapseProjectOverrideMap is specified, it can't be empty.")
    public void emptyProjectOverrideMap() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE)
                .withSynapseProjectOverrideMap(ImmutableMap.of()).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If studyWhitelist is specified, it can't be empty.")
    public void emptyStudyWhitelist() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withStudyWhitelist(ImmutableSet.of()).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If tableWhitelist is specified, it can't be empty.")
    public void emptyTableWhitelist() {
        new BridgeExporterRequest.Builder().withDate(TEST_DATE).withTableWhitelist(ImmutableSet.of()).build();
    }

    @Test
    public void jsonSerializationWithDate() throws Exception {
        // start with JSON
        String jsonText = "{\"date\":\"" + TEST_DATE_STRING + "\"}";

        // convert to POJO
        BridgeExporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeExporterRequest.class);
        assertEquals(request.getDate(), TEST_DATE);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);

        // Convert back to JSON. Also make sure the JSON doesn't have null fields. (Just check record override. No need
        // to check them all.
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(jsonNode.get("date").textValue(), TEST_DATE_STRING);
        assertFalse(jsonNode.has("recordS3Override"));
        assertEquals(jsonNode.get("sharingMode").textValue(), BridgeExporterSharingMode.SHARED.name());
    }

    @Test
    public void jsonSerializationWithOptionalParams() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"exporterDdbPrefixOverride\":\"" + TEST_DDB_PREFIX_OVERRIDE + "\",\n" +
                "   \"recordIdS3Override\":\"" + TEST_RECORD_OVERRIDE + "\",\n" +
                "   \"sharingMode\":\"PUBLIC_ONLY\",\n" +
                "   \"studyWhitelist\":[\"test-study\"],\n" +
                "   \"synapseProjectOverrideMap\":{\n" +
                "       \"test-study\":\"test-project-id\"\n" +
                "   },\n" +
                "   \"tableWhitelist\":[{\n" +
                "       \"studyId\":\"test-study\",\n" +
                "       \"schemaId\":\"test-schema\",\n" +
                "       \"revision\":13\n" +
                "   }],\n" +
                "   \"tag\":\"" + TEST_TAG + "\"\n" +
                "}";

        // convert to POJO
        BridgeExporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeExporterRequest.class);
        assertNull(request.getDate());
        assertEquals(request.getExporterDdbPrefixOverride(), TEST_DDB_PREFIX_OVERRIDE);
        assertEquals(request.getRecordIdS3Override(), TEST_RECORD_OVERRIDE);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.PUBLIC_ONLY);
        assertEquals(request.getStudyWhitelist(), ImmutableSet.of("test-study"));
        assertEquals(request.getSynapseProjectOverrideMap(), TEST_PROJECT_OVERRIDE_MAP);
        assertEquals(request.getTableWhitelist(), ImmutableSet.of(TEST_SCHEMA_KEY));
        assertEquals(request.getTag(), TEST_TAG);

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertFalse(jsonNode.has("date"));
        assertEquals(jsonNode.get("exporterDdbPrefixOverride").textValue(), TEST_DDB_PREFIX_OVERRIDE);
        assertEquals(jsonNode.get("recordIdS3Override").textValue(), TEST_RECORD_OVERRIDE);
        assertEquals(jsonNode.get("sharingMode").textValue(), BridgeExporterSharingMode.PUBLIC_ONLY.name());
        assertEquals(jsonNode.get("tag").textValue(), TEST_TAG);

        JsonNode studyWhitelistNode = jsonNode.get("studyWhitelist");
        assertTrue(studyWhitelistNode.isArray());
        assertEquals(studyWhitelistNode.size(), 1);
        assertEquals(studyWhitelistNode.get(0).textValue(), "test-study");

        JsonNode projectOverrideMapNode = jsonNode.get("synapseProjectOverrideMap");
        assertTrue(projectOverrideMapNode.isObject());
        assertEquals(projectOverrideMapNode.size(), 1);
        assertEquals(projectOverrideMapNode.get("test-study").textValue(), "test-project-id");

        JsonNode tableWhitelistNode = jsonNode.get("tableWhitelist");
        assertTrue(tableWhitelistNode.isArray());
        assertEquals(tableWhitelistNode.size(), 1);
        assertTrue(tableWhitelistNode.get(0).isObject());
        assertEquals(tableWhitelistNode.get(0).size(), 3);
        assertEquals(tableWhitelistNode.get(0).get("studyId").textValue(), "test-study");
        assertEquals(tableWhitelistNode.get(0).get("schemaId").textValue(), "test-schema");
        assertEquals(tableWhitelistNode.get(0).get("revision").intValue(), 13);
    }
}
