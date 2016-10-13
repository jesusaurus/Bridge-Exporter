package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.SortedSetMultimap;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.bridge.sdk.models.healthData.RecordExportStatusRequest;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.Session;
import org.sagebionetworks.bridge.sdk.UploadSchemaClient;
import org.sagebionetworks.bridge.sdk.WorkerClient;
import org.sagebionetworks.bridge.sdk.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchemaType;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    public static final String TEST_SCHEMA_ID = "my-schema";
    public static final int TEST_SCHEMA_REV = 2;
    public static final String TEST_STUDY_ID = "test-study";
    public static final List<String> TEST_RECORD_IDS = Arrays.asList("test record id");
    public static final RecordExportStatusRequest.ExporterStatus TEST_STATUS = RecordExportStatusRequest.ExporterStatus.NOT_EXPORTED;

    public static final String TEST_FIELD_NAME = "my-field";
    public static final UploadFieldDefinition TEST_FIELD_DEF = new UploadFieldDefinition.Builder()
            .withName(TEST_FIELD_NAME).withType(UploadFieldType.STRING).withUnboundedText(true).build();

    public static final ColumnModel TEST_SYNAPSE_COLUMN;
    static {
        TEST_SYNAPSE_COLUMN = new ColumnModel();
        TEST_SYNAPSE_COLUMN.setName(TEST_FIELD_NAME);
        TEST_SYNAPSE_COLUMN.setColumnType(ColumnType.LARGETEXT);
    }

    public static final UploadSchema TEST_SCHEMA = simpleSchemaBuilder().withFieldDefinitions(TEST_FIELD_DEF).build();
    public static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).build();

    public static UploadSchema.Builder simpleSchemaBuilder() {
        return new UploadSchema.Builder().withName("My Schema").withRevision(TEST_SCHEMA_REV)
                .withSchemaId(TEST_SCHEMA_ID).withSchemaType(UploadSchemaType.IOS_DATA).withStudyId(TEST_STUDY_ID);
    }

    private static BridgeHelper setupBridgeHelperWithSession(Session session) {
        // Spy bridge helper, because signIn() statically calls ClientProvider.signIn()
        BridgeHelper bridgeHelper = spy(new BridgeHelper());
        doReturn(session).when(bridgeHelper).signIn();
        return bridgeHelper;
    }

    ArgumentCaptor<RecordExportStatusRequest> requestArgumentCaptor = ArgumentCaptor.forClass(RecordExportStatusRequest.class);


    @Test
    public void completeUpload() {
        // mock worker client, session, and setup bridge helper
        WorkerClient mockWorkerClient = mock(WorkerClient.class);
        Session mockSession = mock(Session.class);
        when(mockSession.getWorkerClient()).thenReturn(mockWorkerClient);
        BridgeHelper bridgeHelper = setupBridgeHelperWithSession(mockSession);

        // execute and verify
        bridgeHelper.completeUpload("test-upload");
        verify(mockWorkerClient).completeUpload("test-upload");
    }

    @Test
    public void updateRecordExporterStatus() {
        // mock worker client, session, and setup bridge helper
        WorkerClient mockWorkerClient = mock(WorkerClient.class);
        Session mockSession = mock(Session.class);
        when(mockSession.getWorkerClient()).thenReturn(mockWorkerClient);
        BridgeHelper bridgeHelper = setupBridgeHelperWithSession(mockSession);

        // execute and verify
        bridgeHelper.updateRecordExporterStatus(TEST_RECORD_IDS, TEST_STATUS);
        verify(mockWorkerClient).updateRecordExporterStatus(anyVararg());
        verify(mockWorkerClient).updateRecordExporterStatus(requestArgumentCaptor.capture());
        RecordExportStatusRequest request = requestArgumentCaptor.getValue();
        assertEquals(request.getRecordIds().get(0), TEST_RECORD_IDS.get(0));
        assertEquals(request.getSynapseExporterStatus(), TEST_STATUS);
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

        // mock session, which returns the schema client
        Session mockSession = mock(Session.class);
        when(mockSession.getUploadSchemaClient()).thenReturn(mockSchemaClient);

        // set up bridge helper
        return setupBridgeHelperWithSession(mockSession);
    }

    @Test
    public void testSessionHelper() throws Exception {
        // 3 test cases:
        // 1. first request initializes session
        // 2. second request re-uses same session
        // 3. third request gets 401'ed, refreshes session
        //
        // This necessitates 4 calls to our test server call (we'll use getSchema):
        // 1. Initial call succeeds.
        // 2. Second call also succeeds.
        // 3. Third call throws 401.
        // 4. Fourth call succeeds, to complete our call pattern.

        // Create 2 mock schema clients.
        // First schema client succeeds twice, then 401s.
        // Second schema client succeeds.
        UploadSchemaClient mockSchemaClient1 = mock(UploadSchemaClient.class);
        when(mockSchemaClient1.getSchema(eq(TEST_STUDY_ID), eq(TEST_SCHEMA_ID), anyInt())).thenReturn(TEST_SCHEMA)
                .thenReturn(TEST_SCHEMA).thenThrow(NotAuthenticatedException.class);

        UploadSchemaClient mockSchemaClient2 = mock(UploadSchemaClient.class);
        when(mockSchemaClient2.getSchema(eq(TEST_STUDY_ID), eq(TEST_SCHEMA_ID), anyInt())).thenReturn(TEST_SCHEMA);

        // Create 2 mock sessions. Each mock session simply returns its corresponing schema client.
        Session mockSession1 = mock(Session.class);
        when(mockSession1.getUploadSchemaClient()).thenReturn(mockSchemaClient1);

        Session mockSession2 = mock(Session.class);
        when(mockSession2.getUploadSchemaClient()).thenReturn(mockSchemaClient2);

        // Spy BridgeHelper.signIn(), which returns these sessions.
        BridgeHelper bridgeHelper = spy(new BridgeHelper());
        doReturn(mockSession1).doReturn(mockSession2).when(bridgeHelper).signIn();

        // Dummy metrics for test.
        Metrics metrics = new Metrics();

        // Call BridgeHelper.getSchema() 3 times. Each call will look identical, except pass in a different rev to
        // bypass the cache.
        for (int i = 1; i <= 3; i++) {
            UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
                    .withSchemaId(TEST_SCHEMA_ID).withRevision(i).build();
            UploadSchema retval = bridgeHelper.getSchema(metrics, schemaKey);
            assertEquals(retval, TEST_SCHEMA);
        }

        // Validate behind the scenes. We made 3 calls to mockSchemaClient1 and 1 call to mockSchemaClient2. This
        // ensures we're properly throwing and catching 401s and refreshing sessions.
        verify(mockSchemaClient1, times(3)).getSchema(eq(TEST_STUDY_ID), eq(TEST_SCHEMA_ID), anyInt());
        verify(mockSchemaClient2, times(1)).getSchema(eq(TEST_STUDY_ID), eq(TEST_SCHEMA_ID), anyInt());
    }
}
