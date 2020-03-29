package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

// Tests for SynapseHelper.uploadFromS3ToSynapseFileHandle() and related methods.
@SuppressWarnings("unchecked")
public class SynapseHelperUploadAttachmentTest {
    private static final int CONTENT_LENGTH = 42;
    private static final String CONTENT_MD5 = "Dummy++Value";
    private static final String CONTENT_TYPE = "text/plain";

    private static final String TEST_ATTACHMENT_ID = "attId";
    private static final String TEST_ATTACHMENTS_BUCKET = "attachments-bucket";
    private static final String TEST_FILE_HANDLE_ID = "file-handle-id";
    private static final String TEST_PROJECT_ID = "project-id";
    private static final long TEST_STORAGE_LOCATION_ID = 1234L;

    private SynapseHelper helper;
    private SynapseClient mockClient;
    private ObjectMetadata s3ObjectMetadata;

    @BeforeMethod
    public void before() {
        // Mock S3 Helper.
        s3ObjectMetadata = new ObjectMetadata();
        s3ObjectMetadata.setContentLength(CONTENT_LENGTH);
        s3ObjectMetadata.setContentType(CONTENT_TYPE);
        s3ObjectMetadata.addUserMetadata(BridgeExporterUtil.KEY_CUSTOM_CONTENT_MD5, CONTENT_MD5);

        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.getObjectMetadata(TEST_ATTACHMENTS_BUCKET, TEST_ATTACHMENT_ID)).thenReturn(s3ObjectMetadata);

        // Mock Synapse Client.
        mockClient = mock(SynapseClient.class);

        // Set up Synapse Helper.
        helper = new SynapseHelper();
        helper.setConfig(mockConfig());
        helper.setS3Helper(mockS3Helper);
        helper.setSynapseClient(mockClient);
    }

    @Test
    public void uploadTest() throws Exception {
        // Mock Synapse Client create file handle call.
        S3FileHandle createdFileHandle = new S3FileHandle();
        createdFileHandle.setId(TEST_FILE_HANDLE_ID);
        when(mockClient.createExternalS3FileHandle(any())).thenReturn(createdFileHandle);

        // execute and validate
        String fileHandleId = helper.uploadFromS3ToSynapseFileHandle(TEST_PROJECT_ID, TEST_ATTACHMENT_ID);
        assertEquals(fileHandleId, TEST_FILE_HANDLE_ID);

        // Validate Synapse call.
        ArgumentCaptor<S3FileHandle> svcInputFileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockClient).createExternalS3FileHandle(svcInputFileHandleCaptor.capture());

        S3FileHandle svcInputFileHandle = svcInputFileHandleCaptor.getValue();
        assertEquals(svcInputFileHandle.getBucketName(), TEST_ATTACHMENTS_BUCKET);
        assertEquals(svcInputFileHandle.getContentMd5(), CONTENT_MD5);
        assertEquals(svcInputFileHandle.getContentSize().intValue(), CONTENT_LENGTH);
        assertEquals(svcInputFileHandle.getContentType(), CONTENT_TYPE);
        assertEquals(svcInputFileHandle.getFileName(), TEST_ATTACHMENT_ID);
        assertEquals(svcInputFileHandle.getKey(), TEST_ATTACHMENT_ID);
        assertEquals(svcInputFileHandle.getStorageLocationId().longValue(), TEST_STORAGE_LOCATION_ID);
    }

    @Test
    public void uploadEmptyAttachment() throws Exception {
        // Mock metadata to have content length 0.
        s3ObjectMetadata.setContentLength(0);

        // execute and validate
        String fileHandleId = helper.uploadFromS3ToSynapseFileHandle(TEST_PROJECT_ID, TEST_ATTACHMENT_ID);
        assertNull(fileHandleId);

        // validate that createS3FileHandle is never called
        verify(mockClient, never()).createExternalS3FileHandle(any());
    }

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(TEST_ATTACHMENTS_BUCKET);
        when(mockConfig.get(SynapseHelper.CONFIG_KEY_SYNAPSE_STORAGE_LOCATION_ID)).thenReturn(
                String.valueOf(TEST_STORAGE_LOCATION_ID));

        // Set a very high number for rate limiting, since we don't want the rate limiter to interfere with our tests.
        when(mockConfig.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND)).thenReturn(1000);
        when(mockConfig.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_GET_COLUMN_MODELS_RATE_LIMIT_PER_MINUTE)).thenReturn(
                1000);

        return mockConfig;
    }
}
