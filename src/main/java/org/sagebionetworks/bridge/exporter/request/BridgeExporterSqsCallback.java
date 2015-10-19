package org.sagebionetworks.bridge.exporter.request;

import java.io.IOException;

import org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;

// TODO doc
public class BridgeExporterSqsCallback implements PollSqsCallback {
    private BridgeExporterRecordProcessor recordProcessor;

    // TODO doc
    public final void setRecordProcessor(BridgeExporterRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void callback(String messageBody) throws IOException {
        BridgeExporterRequest request = DefaultObjectMapper.INSTANCE.readValue(messageBody,
                BridgeExporterRequest.class);
        recordProcessor.processRecordsForRequest(request);
    }
}
