package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.SortedSet;

import com.google.common.collect.SortedSetMultimap;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.UploadSchemaClient;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchemaType;

public class BridgeHelperTest {
    public static final UploadFieldDefinition TEST_FIELD_DEF = new UploadFieldDefinition.Builder().withName("my-field")
            .withType(UploadFieldType.STRING).build();
    public static final String TEST_SCHEMA_ID = "my-schema";
    public static final int TEST_SCHEMA_REV = 2;
    public static final String TEST_STUDY_ID = "test-study";

    public static final UploadSchema TEST_SCHEMA = simpleSchemaBuilder().withFieldDefinitions(TEST_FIELD_DEF).build();
    public static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).build();

    public static UploadSchema.Builder simpleSchemaBuilder() {
        return new UploadSchema.Builder().withName("My Schema").withRevision(TEST_SCHEMA_REV)
                .withSchemaId(TEST_SCHEMA_ID).withSchemaType(UploadSchemaType.IOS_DATA).withStudyId(TEST_STUDY_ID);
    }

    @Test
    public void getSchema() throws Exception {
        // set up bridge helper
        BridgeHelper bridgeHelper = setupBridgeHelperWithSchema(TEST_SCHEMA);

        // execute and validate
        UploadSchema retVal = bridgeHelper.getSchema(new Metrics(), TEST_SCHEMA_KEY);
        assertEquals(retVal, TEST_SCHEMA);
    }

    @Test
    public void getSchemaNotFound() throws Exception {
        // set up bridge helper
        BridgeHelper bridgeHelper = setupBridgeHelperWithSchema(null);

        Metrics metrics = new Metrics();

        // execute and validate
        try {
            bridgeHelper.getSchema(metrics, TEST_SCHEMA_KEY);
            fail("expected exception");
        } catch (SchemaNotFoundException ex) {
            // expected exception
        }

        SortedSetMultimap<String, String> keyValuesMap = metrics.getKeyValuesMap();
        SortedSet<String> schemasNotFoundSet = keyValuesMap.get("schemasNotFound");
        assertEquals(schemasNotFoundSet.size(), 1);
        assertTrue(schemasNotFoundSet.contains(TEST_SCHEMA_KEY.toString()));
    }

    private static BridgeHelper setupBridgeHelperWithSchema(UploadSchema schema) throws Exception {
        // mock schema client
        UploadSchemaClient mockSchemaClient = mock(UploadSchemaClient.class);
        when(mockSchemaClient.getSchema(TEST_STUDY_ID, TEST_SCHEMA_ID, TEST_SCHEMA_REV)).thenReturn(schema);

        // Spy bridge helper, because getSchemaClient() statically calls ClientProvider.signIn()
        BridgeHelper bridgeHelper = spy(new BridgeHelper());
        doReturn(mockSchemaClient).when(bridgeHelper).getSchemaClient();
        return bridgeHelper;
    }
}
