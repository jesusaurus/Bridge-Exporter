package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;

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

        // mock Synapse Client - Mock everything except uploadCsvToTableAsyncGet(), which depends on the test.
        mockSynapseClient = mock(SynapseClient.class);

        FileHandle mockFileHandle = mock(FileHandle.class);
        when(mockFileHandle.getId()).thenReturn(TEST_FILE_HANDLE_ID);
        when(mockSynapseClient.createFileHandle(mockTsvFile, "text/tab-separated-values", TEST_PROJECT_ID))
                .thenReturn(mockFileHandle);

        tableDescCaptor = ArgumentCaptor.forClass(CsvTableDescriptor.class);
        when(mockSynapseClient.uploadCsvToTableAsyncStart(eq(TEST_TABLE_ID), eq(TEST_FILE_HANDLE_ID),
                isNull(String.class), isNull(Long.class), tableDescCaptor.capture())).thenReturn(TEST_JOB_TOKEN);

        synapseHelper = new SynapseHelper();
        synapseHelper.setConfig(config);
        synapseHelper.setSynapseClient(mockSynapseClient);
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
