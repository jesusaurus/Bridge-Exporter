package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Reader;
import java.io.Writer;

import com.google.common.io.CharStreams;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadFieldTypes;

// Tests for SynapseHelper.uploadFromS3ToSynapseFileHandle() and related methods.
@SuppressWarnings("unchecked")
public class SynapseHelperUploadAttachmentTest {
    @Test
    public void filenameJsonBlob() {
        assertEquals(SynapseHelper.generateFilename("test.json.blob", UploadFieldTypes.ATTACHMENT_JSON_BLOB, "attId"),
                "test.json.blob-attId.json");
    }

    @Test
    public void filenameJsonTable() {
        assertEquals(SynapseHelper.generateFilename("test.json.table", UploadFieldTypes.ATTACHMENT_JSON_TABLE,
                "attId"), "test.json.table-attId.json");
    }

    @Test
    public void filenameJsonWithJsonExtension() {
        assertEquals(SynapseHelper.generateFilename("test.json", UploadFieldTypes.ATTACHMENT_JSON_BLOB, "attId"),
                "test-attId.json");
    }

    @Test
    public void filenameCsv() {
        assertEquals(SynapseHelper.generateFilename("csv.data", UploadFieldTypes.ATTACHMENT_CSV, "attId"),
                "csv.data-attId.csv");
    }

    @Test
    public void filenameCsvWithCsvExtension() {
        assertEquals(SynapseHelper.generateFilename("data.csv", UploadFieldTypes.ATTACHMENT_CSV, "attId"),
                "data-attId.csv");
    }

    @Test
    public void filenameBlob() {
        assertEquals(SynapseHelper.generateFilename("generic.blob", UploadFieldTypes.ATTACHMENT_BLOB, "attId"),
                "generic-attId.blob");
    }

    @Test
    public void filenameBlobWithNoExtension() {
        assertEquals(SynapseHelper.generateFilename("genericBlob", UploadFieldTypes.ATTACHMENT_BLOB, "attId"),
                "genericBlob-attId.tmp");
    }

    @Test
    public void filenameBlobWithTmpExtension() {
        assertEquals(SynapseHelper.generateFilename("genericBlob.tmp", UploadFieldTypes.ATTACHMENT_BLOB, "attId"),
                "genericBlob-attId.tmp");
    }

    @Test
    public void filenameBlobWithMultipleDots() {
        assertEquals(SynapseHelper.generateFilename("generic.blob.ext", UploadFieldTypes.ATTACHMENT_BLOB, "attId"),
                "generic.blob-attId.ext");
    }

    private static final String DUMMY_FILE_CONTENT = "This is some file content.";
    private static final String EXPECTED_FILE_NAME = "test-attachment-id.blob";
    private static final String TEST_ATTACHMENT_ID = "attachment-id";
    private static final String TEST_ATTACHMENTS_BUCKET = "attachments-bucket";
    private static final String TEST_FIELD_NAME = "test.blob";
    private static final String TEST_FILE_HANDLE_ID = "file-handle-id";
    private static final String TEST_PROJECT_ID = "project-id";

    @Test
    public void uploadTest() throws Exception {
        // mock (in-memory) file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // Mock Synapse client. Since we delete the file immediately afterwards, use a mock answer to validate file
        // contents.
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        when(mockSynapseClient.createFileHandle(any(), eq("application/octet-stream"), eq(TEST_PROJECT_ID)))
                .thenAnswer(invocation -> {
                    // validate file name and contents
                    File file = invocation.getArgumentAt(0, File.class);
                    assertEquals(file.getName(), EXPECTED_FILE_NAME);
                    try (Reader fileReader = mockFileHelper.getReader(file)) {
                        String fileContent = CharStreams.toString(fileReader);
                        assertEquals(fileContent, DUMMY_FILE_CONTENT);
                    }

                    // Create return value. Only thing we care about is file handle ID.
                    FileHandle fileHandle = mock(FileHandle.class);
                    when(fileHandle.getId()).thenReturn(TEST_FILE_HANDLE_ID);
                    return fileHandle;
                });

        // set up other mocks
        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setConfig(mockConfig());
        synapseHelper.setFileHelper(mockFileHelper);
        synapseHelper.setS3Helper(mockS3Helper(mockFileHelper));
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        String fileHandleId = synapseHelper.uploadFromS3ToSynapseFileHandle(tmpDir, TEST_PROJECT_ID, TEST_FIELD_NAME,
                UploadFieldTypes.ATTACHMENT_BLOB, TEST_ATTACHMENT_ID);
        assertEquals(fileHandleId, TEST_FILE_HANDLE_ID);

        // validate that SynapseHelper cleans up after itself
        mockFileHelper.deleteDir(tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void uploadFailureStillDeletesFiles() throws Exception {
        // mock (in-memory) file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // mock Synapse client to throw exception
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        when(mockSynapseClient.createFileHandle(any(), eq("application/octet-stream"), eq(TEST_PROJECT_ID)))
                .thenThrow(SynapseClientException.class);

        // set up other mocks
        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setConfig(mockConfig());
        synapseHelper.setFileHelper(mockFileHelper);
        synapseHelper.setS3Helper(mockS3Helper(mockFileHelper));
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        try {
            synapseHelper.uploadFromS3ToSynapseFileHandle(tmpDir, TEST_PROJECT_ID, TEST_FIELD_NAME,
                    UploadFieldTypes.ATTACHMENT_BLOB, TEST_ATTACHMENT_ID);
            fail("expected exception");
        } catch (SynapseException ex) {
            // expected exception
        }

        // validate that SynapseHelper cleans up after itself
        mockFileHelper.deleteDir(tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(TEST_ATTACHMENTS_BUCKET);
        return mockConfig;
    }

    private static S3Helper mockS3Helper(FileHelper mockFileHelper) {
        S3Helper mockS3Helper = mock(S3Helper.class);
        doAnswer(invocation -> {
            // write to file to simulate an actual download
            File destFile = invocation.getArgumentAt(2, File.class);
            assertEquals(destFile.getName(), EXPECTED_FILE_NAME);
            try (Writer destFileWriter = mockFileHelper.getWriter(destFile)) {
                destFileWriter.write(DUMMY_FILE_CONTENT);
            }

            // Java doesn't know that we don't need to return anything.
            return null;
        }).when(mockS3Helper).downloadS3File(eq(TEST_ATTACHMENTS_BUCKET), eq(TEST_ATTACHMENT_ID), any());

        return mockS3Helper;
    }
}
