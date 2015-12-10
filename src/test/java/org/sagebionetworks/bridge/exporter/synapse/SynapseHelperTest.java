package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class SynapseHelperTest {
    // Most of these are retry wrappers, but we should test them anyway for branch coverage.

    @Test
    public void appendRowsToTable() throws Exception {
        // mock synapse client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute
        RowSet rowset = new RowSet();
        synapseHelper.appendRowsToTableWithRetry(rowset, "test-table-id");

        // validate
        verify(mockSynapseClient).appendRowsToTable(same(rowset), anyLong(), eq("test-table-id"));
    }

    @Test
    public void createAcl() throws Exception {
        // mock Synapse Client - Unclear whether Synapse client just passes back the input ACL or if it creates a new
        // one. Regardless, don't depend on this implementation. Just return a separate one for tests.
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        AccessControlList inputAcl = new AccessControlList();
        AccessControlList outputAcl = new AccessControlList();
        when(mockSynapseClient.createACL(inputAcl)).thenReturn(outputAcl);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        AccessControlList retVal = synapseHelper.createAclWithRetry(inputAcl);
        assertSame(retVal, outputAcl);
    }

    @Test
    public void createColumnModels() throws Exception {
        // mock Synapse Client - Similarly, return a new output list
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        List<ColumnModel> inputColumnModelList = ImmutableList.of(new ColumnModel());
        List<ColumnModel> outputColumnModelList = ImmutableList.of(new ColumnModel());
        when(mockSynapseClient.createColumnModels(inputColumnModelList)).thenReturn(outputColumnModelList);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        List<ColumnModel> retVal = synapseHelper.createColumnModelsWithRetry(inputColumnModelList);
        assertSame(retVal, outputColumnModelList);
    }

    @Test
    public void createFileHandle() throws Exception {
        // mock Synapse Client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        File mockFile = mock(File.class);
        FileHandle mockFileHandle = mock(FileHandle.class);
        when(mockSynapseClient.createFileHandle(mockFile, "application/mock", "project-id"))
                .thenReturn(mockFileHandle);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        FileHandle retVal = synapseHelper.createFileHandleWithRetry(mockFile, "application/mock", "project-id");
        assertSame(retVal, mockFileHandle);
    }

    @Test
    public void createTable() throws Exception {
        // mock Synapse Client - Similarly, return a new output table
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        TableEntity inputTable = new TableEntity();
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.createEntity(inputTable)).thenReturn(outputTable);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        TableEntity retVal = synapseHelper.createTableWithRetry(inputTable);
        assertSame(retVal, outputTable);
    }

    @Test
    public void downloadFileHandle() throws Exception {
        // mock synapse client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute
        File mockFile = mock(File.class);
        synapseHelper.downloadFileHandleWithRetry("file-handle-id", mockFile);

        // validate
        verify(mockSynapseClient).downloadFromFileHandleTemporaryUrl("file-handle-id", mockFile);
    }

    @Test
    public void getColumnModelsForTable() throws Exception {
        // mock Synapse Client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        List<ColumnModel> outputColumnModelList = ImmutableList.of(new ColumnModel());
        when(mockSynapseClient.getColumnModelsForTableEntity("table-id")).thenReturn(outputColumnModelList);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        List<ColumnModel> retVal = synapseHelper.getColumnModelsForTableWithRetry("table-id");
        assertSame(retVal, outputColumnModelList);
    }

    @Test
    public void getTable() throws Exception {
        // mock Synapse Client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        TableEntity tableEntity = new TableEntity();
        when(mockSynapseClient.getEntity("table-id", TableEntity.class)).thenReturn(tableEntity);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        TableEntity retVal = synapseHelper.getTableWithRetry("table-id");
        assertSame(retVal, tableEntity);
    }

    @Test
    public void uploadTsvStart() throws Exception {
        // mock Synapse Client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        CsvTableDescriptor inputTableDesc = new CsvTableDescriptor();
        when(mockSynapseClient.uploadCsvToTableAsyncStart("table-id", "file-handle-id", null, null, inputTableDesc))
                .thenReturn("job-token");

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        String retVal = synapseHelper.uploadTsvStartWithRetry("table-id", "file-handle-id", inputTableDesc);
        assertEquals(retVal, "job-token");
    }

    @Test
    public void uploadTsvStatus() throws Exception {
        // mock SynapseClient
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        UploadToTableResult result = new UploadToTableResult();
        when(mockSynapseClient.uploadCsvToTableAsyncGet("job-token", "table-id")).thenReturn(result);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        UploadToTableResult retVal = synapseHelper.getUploadTsvStatus("job-token", "table-id");
        assertSame(retVal, result);
    }

    @Test
    public void uploadTsvStatusNotReady() throws Exception {
        // mock SynapseClient
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        when(mockSynapseClient.uploadCsvToTableAsyncGet("job-token", "table-id"))
                .thenThrow(SynapseResultNotReadyException.class);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        UploadToTableResult retVal = synapseHelper.getUploadTsvStatus("job-token", "table-id");
        assertNull(retVal);
    }
}
