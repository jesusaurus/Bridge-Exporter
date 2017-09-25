package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SortedSetMultimap;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.RecordExportStatusRequest;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    public static final String TEST_SCHEMA_ID = "my-schema";
    public static final int TEST_SCHEMA_REV = 2;
    public static final SignIn TEST_SIGN_IN = new SignIn();
    public static final String TEST_STUDY_ID = "test-study";
    public static final List<String> TEST_RECORD_IDS = ImmutableList.of("test record id");
    public static final SynapseExporterStatus TEST_STATUS = SynapseExporterStatus.NOT_EXPORTED;

    public static final String TEST_FIELD_NAME = "my-field";
    public static final UploadFieldDefinition TEST_FIELD_DEF = new UploadFieldDefinition()
            .name(TEST_FIELD_NAME).type(UploadFieldType.STRING).unboundedText(true);

    public static final ColumnModel TEST_SYNAPSE_COLUMN;
    static {
        TEST_SYNAPSE_COLUMN = new ColumnModel();
        TEST_SYNAPSE_COLUMN.setName(TEST_FIELD_NAME);
        TEST_SYNAPSE_COLUMN.setColumnType(ColumnType.LARGETEXT);
    }

    public static final UploadSchema TEST_SCHEMA = simpleSchemaBuilder().fieldDefinitions(ImmutableList.of(
            TEST_FIELD_DEF));
    public static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY_ID)
            .withSchemaId(TEST_SCHEMA_ID).withRevision(TEST_SCHEMA_REV).build();

    public BridgeHelper bridgeHelper;
    public AuthenticationApi mockAuthApi;
    public ForWorkersApi mockWorkersApi;

    public static UploadSchema simpleSchemaBuilder() {
        return new UploadSchema().name("My Schema").revision((long) TEST_SCHEMA_REV)
                .schemaId(TEST_SCHEMA_ID).schemaType(UploadSchemaType.IOS_DATA).studyId(TEST_STUDY_ID);
    }

    @BeforeMethod
    public void setup() {
        mockAuthApi = mock(AuthenticationApi.class);
        mockWorkersApi = mock(ForWorkersApi.class);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(AuthenticationApi.class)).thenReturn(mockAuthApi);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkersApi);

        bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);
        bridgeHelper.setBridgeCredentials(TEST_SIGN_IN);
    }

    @Test
    public void completeUpload() throws Exception {
        // mock call
        Call<Message> mockCall = mock(Call.class);
        when(mockWorkersApi.completeUploadSession("test-upload")).thenReturn(mockCall);

        // execute and verify
        bridgeHelper.completeUpload("test-upload");
        verify(mockCall).execute();
    }

    @Test
    public void updateRecordExporterStatus() throws Exception {
        // mock call
        Call<Message> mockCall = mock(Call.class);
        ArgumentCaptor<RecordExportStatusRequest> requestArgumentCaptor = ArgumentCaptor.forClass(
                RecordExportStatusRequest.class);
        when(mockWorkersApi.updateRecordExportStatuses(requestArgumentCaptor.capture())).thenReturn(mockCall);

        // execute and verify
        bridgeHelper.updateRecordExporterStatus(TEST_RECORD_IDS, TEST_STATUS);
        verify(mockCall).execute();

        RecordExportStatusRequest request = requestArgumentCaptor.getValue();
        assertEquals(request.getRecordIds(), TEST_RECORD_IDS);
        assertEquals(request.getSynapseExporterStatus(), TEST_STATUS);
    }

    @Test
    public void getSchema() throws Exception {
        // set up bridge helper
        setupBridgeHelperWithSchema(TEST_SCHEMA);

        // execute and validate
        UploadSchema retVal = bridgeHelper.getSchema(new Metrics(), TEST_SCHEMA_KEY);
        assertEquals(retVal, TEST_SCHEMA);
    }

    @Test
    public void getSchemaNotFound() throws Exception {
        // set up bridge helper
        setupBridgeHelperWithSchema(null);

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

    private void setupBridgeHelperWithSchema(UploadSchema schema) throws Exception {
        Response<UploadSchema> response = Response.success(schema);

        Call<UploadSchema> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkersApi.getSchemaRevisionInStudy(TEST_STUDY_ID, TEST_SCHEMA_ID, (long) TEST_SCHEMA_REV))
                .thenReturn(mockCall);
    }

    @Test
    public void getStudy() throws Exception {
        // mock Bridge Client
        Study testStudy = new Study().identifier(TEST_STUDY_ID);
        Response<Study> response = Response.success(testStudy);

        Call<Study> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        when(mockWorkersApi.getStudy(TEST_STUDY_ID)).thenReturn(mockCall);

        // execute and validate
        Study retVal = bridgeHelper.getStudy(TEST_STUDY_ID);
        assertSame(retVal, testStudy);
    }

    @Test
    public void testSessionHelper() throws Exception {
        // 3 test cases:
        // 1. first request initializes session
        // 2. second request re-uses same session
        // 3. third request gets 401'ed, refreshes session
        //
        // This necessitates 4 calls to our test server call (we'll use completeUpload):
        // 1. Initial call succeeds.
        // 2. Second call also succeeds.
        // 3. Third call throws 401.
        // 4. Fourth call succeeds, to complete our call pattern.

        // Mock ForWorkersApi - third call throws
        Call<Message> mockUploadCall1 = mock(Call.class);
        Call<Message> mockUploadCall2 = mock(Call.class);
        Call<Message> mockUploadCall3a = mock(Call.class);
        Call<Message> mockUploadCall3b = mock(Call.class);
        when(mockUploadCall3a.execute()).thenThrow(NotAuthenticatedException.class);

        when(mockWorkersApi.completeUploadSession("upload1")).thenReturn(mockUploadCall1);
        when(mockWorkersApi.completeUploadSession("upload2")).thenReturn(mockUploadCall2);
        when(mockWorkersApi.completeUploadSession("upload3")).thenReturn(mockUploadCall3a, mockUploadCall3b);

        // Mock AuthenticationApi
        Call<UserSessionInfo> mockSignInCall = mock(Call.class);
        when(mockAuthApi.signIn(TEST_SIGN_IN)).thenReturn(mockSignInCall);

        // execute
        bridgeHelper.completeUpload("upload1");
        bridgeHelper.completeUpload("upload2");
        bridgeHelper.completeUpload("upload3");

        // Validate behind the scenes, in order.
        InOrder inOrder = inOrder(mockUploadCall1, mockUploadCall2, mockUploadCall3a, mockSignInCall,
                mockUploadCall3b);
        inOrder.verify(mockUploadCall1).execute();
        inOrder.verify(mockUploadCall2).execute();
        inOrder.verify(mockUploadCall3a).execute();
        inOrder.verify(mockSignInCall).execute();
        inOrder.verify(mockUploadCall3b).execute();
    }
}
