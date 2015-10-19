package org.sagebionetworks.bridge.exporter.metrics;

import com.amazonaws.services.dynamodbv2.document.Item;

// TODO doc
public class MetricsHelper {
    public void captureMetricsForRecord(Metrics metrics, Item record) {
        metrics.incrementSetCounter("uniqueHealthCodes[" + record.getString("studyId") + "]",
                record.getString("healthCode"));
    }
}
