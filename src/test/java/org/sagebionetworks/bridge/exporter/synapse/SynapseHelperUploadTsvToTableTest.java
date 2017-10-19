package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

// Tests for SynapseHelper.uploadTsvFileToTable() and related methods.
@SuppressWarnings("unchecked")
public class SynapseHelperUploadTsvToTableTest {
    private static final String TEST_FILE_HANDLE_ID = "file-handle-id";
    private static final String TEST_JOB_TOKEN = "job-token";
    private static final String TEST_PROJECT_ID = "project-id";
    private static final String TEST_TABLE_ID = "table-id";

    private File mockTsvFile;
    private SynapseClient mockSynapseClient;
    private SynapseHelper synapseHelper;
    private ArgumentCaptor<CsvTableDescriptor> tableDescCaptor;

    @BeforeMethod
    public void before() throws Exception {
        // mock TSV
        mockTsvFile = mock(File.class);

        // mock config
        Config config = mock(Config.class);
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS)).thenReturn(0);
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS)).thenReturn(2);
        // Set a very high number for rate limiting, since we don't want the rate limiter to interfere with our tests.
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND)).thenReturn(1000);
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_GET_COLUMN_MODELS_RATE_LIMIT_PER_MINUTE)).thenReturn(1000);

        // mock Synapse Client - Mock everything except uploadCsvToTableAsyncGet(), which depends on the test.
        mockSynapseClient = mock(SynapseClient.class);

        tableDescCaptor = ArgumentCaptor.forClass(CsvTableDescriptor.class);
        when(mockSynapseClient.uploadCsvToTableAsyncStart(eq(TEST_TABLE_ID), eq(TEST_FILE_HANDLE_ID),
                isNull(String.class), isNull(Long.class), tableDescCaptor.capture(), isNull(List.class))).thenReturn(
                TEST_JOB_TOKEN);

        synapseHelper = spy(new SynapseHelper());
        synapseHelper.setConfig(config);
        synapseHelper.setSynapseClient(mockSynapseClient);

        // Spy createFileHandle. This is tested somewhere else, and spying it here means we don't have to change tests
        // in 3 different places when we change the createFileHandle implementation.
        FileHandle mockFileHandle = mock(FileHandle.class);
        when(mockFileHandle.getId()).thenReturn(TEST_FILE_HANDLE_ID);
        doReturn(mockFileHandle).when(synapseHelper).createFileHandleWithRetry(mockTsvFile,
                "text/tab-separated-values", TEST_PROJECT_ID);
    }

    @Test
    public void normalCase() throws Exception {
        // mock synapseClient.uploadCsvToTableAsyncGet() - first loop not ready, second loop has results
        UploadToTableResult uploadTsvStatus = new UploadToTableResult();
        uploadTsvStatus.setRowsProcessed(42L);
        when(mockSynapseClient.uploadCsvToTableAsyncGet(TEST_JOB_TOKEN, TEST_TABLE_ID)).thenThrow(
                SynapseResultNotReadyException.class).thenReturn(uploadTsvStatus);

        // execute and validate
        long linesProcessed = synapseHelper.uploadTsvFileToTable(TEST_PROJECT_ID, TEST_TABLE_ID, mockTsvFile);
        assertEquals(linesProcessed, 42);

        // validate CsvTableDescriptor
        CsvTableDescriptor tableDesc = tableDescCaptor.getValue();
        assertTrue(tableDesc.getIsFirstLineHeader());
        assertEquals(tableDesc.getSeparator(), "\t");
    }

    @Test(expectedExceptions = BridgeExporterException.class, expectedExceptionsMessageRegExp =
            "Timed out uploading file handle " + TEST_FILE_HANDLE_ID)
    public void timeout() throws Exception {
        // mock synapseClient.uploadCsvToTableAsyncGet() to throw
        when(mockSynapseClient.uploadCsvToTableAsyncGet(TEST_JOB_TOKEN, TEST_TABLE_ID)).thenThrow(
                SynapseResultNotReadyException.class);

        // execute
        synapseHelper.uploadTsvFileToTable(TEST_PROJECT_ID, TEST_TABLE_ID, mockTsvFile);
    }

    @Test(expectedExceptions = BridgeExporterException.class, expectedExceptionsMessageRegExp = "Null rows processed")
    public void nullGetRowsProcessed() throws Exception{
        // mock synapseClient.uploadCsvToTableAsyncGet()
        UploadToTableResult uploadTsvStatus = new UploadToTableResult();
        uploadTsvStatus.setRowsProcessed(null);
        when(mockSynapseClient.uploadCsvToTableAsyncGet(TEST_JOB_TOKEN, TEST_TABLE_ID)).thenReturn(uploadTsvStatus);

        // execute
        synapseHelper.uploadTsvFileToTable(TEST_PROJECT_ID, TEST_TABLE_ID, mockTsvFile);
    }
}
