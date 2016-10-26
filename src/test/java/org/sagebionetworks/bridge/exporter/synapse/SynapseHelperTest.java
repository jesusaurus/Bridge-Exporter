package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.sagebionetworks.repo.model.file.UploadDestination;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SynapseHelperTest {
    @DataProvider
    public Object[][] maxLengthTestDataProvider() {
        // { fieldDef, expectedMaxLength }
        return new Object[][] {
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.CALENDAR_DATE)
                        .build(), 10 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.DURATION_V2).build(),
                        24 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.INLINE_JSON_BLOB)
                        .build(), 100 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.SINGLE_CHOICE)
                        .build(), 100 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.STRING).build(),
                        100 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.TIME_V2).build(),
                        12 },
                { new UploadFieldDefinition.Builder().withName("dummy").withType(UploadFieldType.STRING)
                        .withMaxLength(256).build(), 256 },
        };
    }

    @Test(dataProvider = "maxLengthTestDataProvider")
    public void getMaxLengthForFieldDef(UploadFieldDefinition fieldDef, int expectedMaxLength) {
        int retVal = SynapseHelper.getMaxLengthForFieldDef(fieldDef);
        assertEquals(retVal, expectedMaxLength);
    }

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

        UploadDestination mockUploadDestination = mock(UploadDestination.class);
        when(mockUploadDestination.getStorageLocationId()).thenReturn(1234L);
        when(mockSynapseClient.getDefaultUploadDestination("project-id")).thenReturn(mockUploadDestination);

        File mockFile = mock(File.class);
        S3FileHandle mockFileHandle = mock(S3FileHandle.class);
        when(mockSynapseClient.multipartUpload(mockFile, 1234L, null, null)).thenReturn(mockFileHandle);

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
    public void createTableWithColumnsAndAcls() throws Exception {
        // Set up inputs. This table will have two columns. We pass this straight through to the create column call,
        // and we never look inside, so don't bother actually instantiating the columns.
        List<ColumnModel> columnList = ImmutableList.of(new ColumnModel(), new ColumnModel());

        // Spy SynapseHelper. This way, we can test the logic in SynapseHelper without being tightly coupled to the
        // imeplementations of create column, create table, and create ACLs.
        SynapseHelper synapseHelper = spy(new SynapseHelper());

        // mock create column call - We only care about the IDs, so don't bother instantiating the rest.
        ColumnModel createdFooColumn = new ColumnModel();
        createdFooColumn.setId("foo-col-id");

        ColumnModel createdBarColumn = new ColumnModel();
        createdBarColumn.setId("bar-col-id");

        List<ColumnModel> createdColumnList = ImmutableList.of(createdFooColumn, createdBarColumn);

        doReturn(createdColumnList).when(synapseHelper).createColumnModelsWithRetry(columnList);

        // mock create table call - We only care about the table ID, so don't bother instantiating the rest.
        TableEntity createdTable = new TableEntity();
        createdTable.setId("test-table");

        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        doReturn(createdTable).when(synapseHelper).createTableWithRetry(tableCaptor.capture());

        // Mock create ACL call. Even though we don't care about the return value, we have to do it, otherwise it'll
        // call through to the real method. Likewise, for the result, just return a dummy object. We never look at it
        // anyway.
        ArgumentCaptor<AccessControlList> aclCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        doReturn(new AccessControlList()).when(synapseHelper).createAclWithRetry(aclCaptor.capture());

        // execute and validate
        String retVal = synapseHelper.createTableWithColumnsAndAcls(columnList, /*data access team ID*/ 1234,
                /*principal ID*/ 5678, "test-project", "My Table");
        assertEquals(retVal, "test-table");

        // validate tableCaptor
        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getName(), "My Table");
        assertEquals(table.getParentId(), "test-project");
        assertEquals(table.getColumnIds(), ImmutableList.of("foo-col-id", "bar-col-id"));

        // validate aclCaptor
        AccessControlList acl = aclCaptor.getValue();
        assertEquals(acl.getId(), "test-table");

        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();
        assertEquals(resourceAccessSet.size(), 2);

        ResourceAccess exporterOwnerAccess = new ResourceAccess();
        exporterOwnerAccess.setPrincipalId(5678L);
        exporterOwnerAccess.setAccessType(SynapseHelper.ACCESS_TYPE_ALL);
        assertTrue(resourceAccessSet.contains(exporterOwnerAccess));

        ResourceAccess dataAccessTeamAccess = new ResourceAccess();
        dataAccessTeamAccess.setPrincipalId(1234L);
        dataAccessTeamAccess.setAccessType(SynapseHelper.ACCESS_TYPE_READ);
        assertTrue(resourceAccessSet.contains(dataAccessTeamAccess));
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

    @DataProvider(name = "isSynapseWritableProvider")
    public Object[][] isSynapseWritableProvider() {
        // { status, expected }
        return new Object[][] {
                { StatusEnum.READ_WRITE, true },
                { StatusEnum.READ_ONLY, false },
                { StatusEnum.DOWN, false },
        };
    }

    @Test(dataProvider = "isSynapseWritableProvider")
    public void isSynapseWritable(StatusEnum status, boolean expected) throws Exception {
        // mock synapse client
        StackStatus stackStatus = new StackStatus();
        stackStatus.setStatus(status);

        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        when(mockSynapseClient.getCurrentStackStatus()).thenReturn(stackStatus);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        boolean retVal = synapseHelper.isSynapseWritable();
        assertEquals(retVal, expected);
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
    public void updateTable() throws Exception {
        // mock Synapse Client - Assume put entity returns a separate output table.
        SynapseClient mockSynapseClient = mock(SynapseClient.class);
        TableEntity inputTable = new TableEntity();
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.putEntity(inputTable)).thenReturn(outputTable);

        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(mockSynapseClient);

        // execute and validate
        TableEntity retVal = synapseHelper.updateTableWithRetry(inputTable);
        assertSame(retVal, outputTable);
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
