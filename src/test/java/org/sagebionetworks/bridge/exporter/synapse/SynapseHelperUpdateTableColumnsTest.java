package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.sagebionetworks.repo.model.table.TableSchemaChangeResponse;
import org.sagebionetworks.repo.model.table.TableUpdateRequest;
import org.sagebionetworks.repo.model.table.TableUpdateResponse;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseHelperUpdateTableColumnsTest {
    private static final TableSchemaChangeRequest DUMMY_REQUEST = new TableSchemaChangeRequest();
    private static final String JOB_TOKEN = "job-token";
    private static final String TABLE_ID = "table-id";

    private ArgumentCaptor<List> changeListCaptor;
    private SynapseClient mockSynapseClient;
    private SynapseHelper synapseHelper;

    @BeforeMethod
    public void setup() throws Exception {
        // mock config
        Config config = mock(Config.class);
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS)).thenReturn(0);
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS)).thenReturn(2);
        // Set a very high number for rate limiting, since we don't want the rate limiter to interfere with our tests.
        when(config.getInt(SynapseHelper.CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND)).thenReturn(1000);

        // mock Synapse Client and startTableTransactionJob
        mockSynapseClient = mock(SynapseClient.class);

        changeListCaptor = ArgumentCaptor.forClass(List.class);
        when(mockSynapseClient.startTableTransactionJob(changeListCaptor.capture(), eq(TABLE_ID))).thenReturn(
                JOB_TOKEN);

        // create Synapse Helper
        synapseHelper = new SynapseHelper();
        synapseHelper.setConfig(config);
        synapseHelper.setSynapseClient(mockSynapseClient);
    }

    @Test
    public void immediateSuccess() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        TableSchemaChangeResponse singleResponse = new TableSchemaChangeResponse();
        List<TableUpdateResponse> responseList = ImmutableList.of(singleResponse);
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID)).thenReturn(responseList);

        // execute and validate
        TableSchemaChangeResponse retVal = synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
        assertSame(retVal, singleResponse);
        verify(mockSynapseClient, times(1)).getTableTransactionJobResults(any(), any());
        validateInputList();
    }

    @Test
    public void secondTry() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        TableSchemaChangeResponse singleResponse = new TableSchemaChangeResponse();
        List<TableUpdateResponse> responseList = ImmutableList.of(singleResponse);
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class).thenReturn(responseList);

        // execute and validate
        TableSchemaChangeResponse retVal = synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
        assertSame(retVal, singleResponse);
        verify(mockSynapseClient, times(2)).getTableTransactionJobResults(any(), any());
        validateInputList();
    }

    @Test
    public void timeout() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class);

        // execute and validate
        try {
            synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertEquals(ex.getMessage(), "Timed out updating table columns for table " + TABLE_ID);
        }
        verify(mockSynapseClient, times(2)).getTableTransactionJobResults(any(), any());
        validateInputList();
    }

    @Test
    public void emptyResponseList() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID)).thenReturn(ImmutableList.of());

        // execute and validate
        try {
            synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertEquals(ex.getMessage(), "Expected one table update response for table " + TABLE_ID + ", but got 0");
        }
        validateInputList();
    }

    @Test
    public void multipleResponses() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID)).thenReturn(ImmutableList.of(
                new TableSchemaChangeResponse(), new TableSchemaChangeResponse()));

        // execute and validate
        try {
            synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertEquals(ex.getMessage(), "Expected one table update response for table " + TABLE_ID + ", but got 2");
        }
        validateInputList();
    }

    @Test
    public void responseNotSchemaChange() throws Exception {
        // mock SynapseClient.getTableTransationJobResults
        when(mockSynapseClient.getTableTransactionJobResults(JOB_TOKEN, TABLE_ID)).thenReturn(ImmutableList.of(
                new UploadToTableResult()));

        // execute and validate
        try {
            synapseHelper.updateTableColumns(DUMMY_REQUEST, TABLE_ID);
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertEquals(ex.getMessage(), "Expected a TableSchemaChangeResponse for table " + TABLE_ID + ", but got " +
                    UploadToTableResult.class.getName());
        }
        validateInputList();
    }

    // This is done as a helper method instead of an After, so we can know which test failed.
    private void validateInputList() {
        List<TableUpdateRequest> changeList = changeListCaptor.getValue();
        assertEquals(changeList.size(), 1);
        assertSame(changeList.get(0), DUMMY_REQUEST);
    }
}
