package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Reader;
import java.io.Writer;

import com.google.common.io.CharStreams;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.s3.S3Helper;

// Tests for SynapseHelper.uploadFromS3ToSynapseFileHandle() and related methods.
@SuppressWarnings("unchecked")
public class SynapseHelperUploadAttachmentTest {
    private static final String TEST_ATTACHMENT_ID = "attId";

    @Test
    public void filenameFromFieldDef() {
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name("test.foo")
                .type(UploadFieldType.ATTACHMENT_V2).fileExtension(".foo");
        String filename = SynapseHelper.generateFilename(fieldDef, TEST_ATTACHMENT_ID);
        assertEquals(filename, "test-attId.foo");
    }

    @Test
    public void filenameFromFieldDefMismatchedExtension() {
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name("test.bar")
                .type(UploadFieldType.ATTACHMENT_V2).fileExtension(".foo");
        String filename = SynapseHelper.generateFilename(fieldDef, TEST_ATTACHMENT_ID);
        assertEquals(filename, "test.bar-attId.foo");
    }

    @Test
    public void filenameJsonBlob() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("test.json.blob")
            .type(UploadFieldType.ATTACHMENT_JSON_BLOB), TEST_ATTACHMENT_ID), "test.json.blob-attId.json");
    }

    @Test
    public void filenameJsonTable() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("test.json.table")
                        .type(UploadFieldType.ATTACHMENT_JSON_TABLE), TEST_ATTACHMENT_ID),
                "test.json.table-attId.json");
    }

    @Test
    public void filenameJsonWithJsonExtension() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("test.json")
                .type(UploadFieldType.ATTACHMENT_JSON_BLOB), TEST_ATTACHMENT_ID), "test-attId.json");
    }

    @Test
    public void filenameCsv() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("csv.data")
                .type(UploadFieldType.ATTACHMENT_CSV), TEST_ATTACHMENT_ID), "csv.data-attId.csv");
    }

    @Test
    public void filenameCsvWithCsvExtension() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("data.csv")
                .type(UploadFieldType.ATTACHMENT_CSV), TEST_ATTACHMENT_ID), "data-attId.csv");
    }

    @Test
    public void filenameBlob() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("generic.blob")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), "generic-attId.blob");
    }

    @Test
    public void filenameBlobWithNoExtension() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("genericBlob")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), "genericBlob-attId");
    }

    @Test
    public void filenameBlobWithMultipleDots() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("generic.blob.ext")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), "generic.blob-attId.ext");
    }

    @Test
    public void filenameBlobStartsWithDot() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name(".dot")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), ".dot-attId");
    }

    @Test
    public void filenameBlobStartsWithDotWithExtension() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name(".dot.ext")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), ".dot-attId.ext");
    }

    @Test
    public void filenameBlobEndsWithDot() {
        assertEquals(SynapseHelper.generateFilename(new UploadFieldDefinition().name("dot.dot.")
                .type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID), "dot.dot.-attId");
    }

    private static final String DUMMY_FILE_CONTENT = "This is some file content.";
    private static final String TEST_ATTACHMENTS_BUCKET = "attachments-bucket";
    private static final String TEST_FILE_HANDLE_ID = "file-handle-id";
    private static final String TEST_PROJECT_ID = "project-id";

    @DataProvider(name = "uploadTestDataProvider")
    public Object[][] uploadTestDataProvider() {
        // { fieldDef, expectedFilename, expectedMimeType }
        return new Object[][] {
                { new UploadFieldDefinition().name("foo.blob").type(UploadFieldType.ATTACHMENT_BLOB),"foo-attId.blob",
                        "application/octet-stream" },
                { new UploadFieldDefinition().name("bar.csv").type(UploadFieldType.ATTACHMENT_CSV), "bar-attId.csv",
                        "text/csv" },
                { new UploadFieldDefinition().name("baz.json").type(UploadFieldType.ATTACHMENT_JSON_BLOB),
                        "baz-attId.json", "text/json" },
                { new UploadFieldDefinition().name("qwerty.json").type(UploadFieldType.ATTACHMENT_JSON_TABLE),
                        "qwerty-attId.json", "text/json" },
                { new UploadFieldDefinition().name("asdf.file").type(UploadFieldType.ATTACHMENT_V2), "asdf-attId.file",
                        "application/octet-stream" },
                { new UploadFieldDefinition().name("jkl.txt").type(UploadFieldType.ATTACHMENT_V2).fileExtension(".txt")
                        .mimeType("text/plain"), "jkl-attId.txt", "text/plain" },
        };
    }

    @Test(dataProvider = "uploadTestDataProvider")
    public void uploadTest(UploadFieldDefinition fieldDef, String expectedFilename, String expectedMimeType)
            throws Exception {
        // mock (in-memory) file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // set up other mocks
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        synapseHelper.setConfig(mockConfig());
        synapseHelper.setFileHelper(mockFileHelper);
        synapseHelper.setS3Helper(mockS3Helper(mockFileHelper, expectedFilename, DUMMY_FILE_CONTENT));

        // Spy createFileHandle. This is tested somewhere else, and spying it here means we don't have to change tests
        // in 3 different places when we change the createFileHandle implementation.
        // Since we delete the file immediately afterwards, use a mock answer to validate file contents.
        doAnswer(invocation -> {
            // validate file name and contents
            File file = invocation.getArgumentAt(0, File.class);
            assertEquals(file.getName(), expectedFilename);
            try (Reader fileReader = mockFileHelper.getReader(file)) {
                String fileContent = CharStreams.toString(fileReader);
                assertEquals(fileContent, DUMMY_FILE_CONTENT);
            }

            // Create return value. Only thing we care about is file handle ID.
            FileHandle fileHandle = mock(FileHandle.class);
            when(fileHandle.getId()).thenReturn(TEST_FILE_HANDLE_ID);
            return fileHandle;
        }).when(synapseHelper).createFileHandleWithRetry(any(), eq(expectedMimeType), eq(TEST_PROJECT_ID));

        // execute and validate
        String fileHandleId = synapseHelper.uploadFromS3ToSynapseFileHandle(tmpDir, TEST_PROJECT_ID,
                fieldDef, TEST_ATTACHMENT_ID);
        assertEquals(fileHandleId, TEST_FILE_HANDLE_ID);

        // validate that SynapseHelper cleans up after itself
        mockFileHelper.deleteDir(tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void uploadEmptyAttachment() throws Exception {
        // mock (in-memory) file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // set up other mocks
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        synapseHelper.setConfig(mockConfig());
        synapseHelper.setFileHelper(mockFileHelper);
        synapseHelper.setS3Helper(mockS3Helper(mockFileHelper, "test-attId.blob", ""));

        // execute and validate
        String fileHandleId = synapseHelper.uploadFromS3ToSynapseFileHandle(tmpDir, TEST_PROJECT_ID,
                new UploadFieldDefinition().name("test.blob").type(UploadFieldType.ATTACHMENT_BLOB),
                TEST_ATTACHMENT_ID);
        assertNull(fileHandleId);

        // validate that createFileHandle is never called
        verify(synapseHelper, never()).createFileHandleWithRetry(any(), any() ,any());

        // validate that SynapseHelper cleans up after itself
        mockFileHelper.deleteDir(tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void uploadFailureStillDeletesFiles() throws Exception {
        // mock (in-memory) file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // set up other mocks
        SynapseHelper synapseHelper = spy(new SynapseHelper());
        synapseHelper.setConfig(mockConfig());
        synapseHelper.setFileHelper(mockFileHelper);
        synapseHelper.setS3Helper(mockS3Helper(mockFileHelper, "test-attId.blob", DUMMY_FILE_CONTENT));

        // Spy createFileHandle. This is tested somewhere else, and spying it here means we don't have to change tests
        // in 3 different places when we change the createFileHandle implementation.
        // Mock to throw exception.
        doThrow(SynapseClientException.class).when(synapseHelper).createFileHandleWithRetry(any(),
                eq("application/octet-stream"), eq(TEST_PROJECT_ID));

        // execute and validate
        try {
            synapseHelper.uploadFromS3ToSynapseFileHandle(tmpDir, TEST_PROJECT_ID, new UploadFieldDefinition()
                    .name("test.blob").type(UploadFieldType.ATTACHMENT_BLOB), TEST_ATTACHMENT_ID);
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
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(TEST_ATTACHMENTS_BUCKET);

        // Set a very high number for rate limiting, since we don't want the rate limiter to interfere with our tests.
        when(mockConfig.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND)).thenReturn(1000);

        return mockConfig;
    }

    private static S3Helper mockS3Helper(FileHelper mockFileHelper, String expectedFilename, String fileContent) {
        S3Helper mockS3Helper = mock(S3Helper.class);
        doAnswer(invocation -> {
            // write to file to simulate an actual download
            File destFile = invocation.getArgumentAt(2, File.class);
            assertEquals(destFile.getName(), expectedFilename);
            try (Writer destFileWriter = mockFileHelper.getWriter(destFile)) {
                destFileWriter.write(fileContent);
            }

            // Java doesn't know that we don't need to return anything.
            return null;
        }).when(mockS3Helper).downloadS3File(eq(TEST_ATTACHMENTS_BUCKET), eq(TEST_ATTACHMENT_ID), any());

        return mockS3Helper;
    }
}
