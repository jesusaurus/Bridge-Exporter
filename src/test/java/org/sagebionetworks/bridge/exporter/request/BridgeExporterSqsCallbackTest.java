package org.sagebionetworks.bridge.exporter.request;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor;

public class BridgeExporterSqsCallbackTest {
    @Test
    public void test() throws Exception {
        // Basic test that tests data flow. JSON parsing is already tested by BridgeExporterRequestTest.

        // set up test callback
        BridgeExporterRecordProcessor mockRecordProcessor = mock(BridgeExporterRecordProcessor.class);

        BridgeExporterSqsCallback callback = new BridgeExporterSqsCallback();
        callback.setRecordProcessor(mockRecordProcessor);

        // execute and verify
        callback.callback("{\"date\":\"2015-12-01\"}");

        ArgumentCaptor<BridgeExporterRequest> requestCaptor = ArgumentCaptor.forClass(BridgeExporterRequest.class);
        verify(mockRecordProcessor).processRecordsForRequest(requestCaptor.capture());
        BridgeExporterRequest request = requestCaptor.getValue();
        assertEquals(request.getDate().toString(), "2015-12-01");
    }
}
