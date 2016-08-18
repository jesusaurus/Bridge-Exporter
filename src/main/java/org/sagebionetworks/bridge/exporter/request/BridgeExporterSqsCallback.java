package org.sagebionetworks.bridge.exporter.request;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.RestartBridgeExporterException;
import org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/**
 * Responds to SQS messages. This is a pass-through to the BridgeExporterRecordProcessor and only does JSON parsing.
 * This allows us to keep the RecordProcessor's interface clean.
 */
@Component
public class BridgeExporterSqsCallback implements PollSqsCallback {
    private BridgeExporterRecordProcessor recordProcessor;

    /** Record processor, which this class passes the parsed message to. */
    @Autowired
    public final void setRecordProcessor(BridgeExporterRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    /** Parses the SQS message and passes it to the record processor. */
    @Override
    public void callback(String messageBody) throws IOException, PollSqsWorkerBadRequestException,
            RestartBridgeExporterException {
        BridgeExporterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.readValue(messageBody, BridgeExporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        recordProcessor.processRecordsForRequest(request);
    }
}
