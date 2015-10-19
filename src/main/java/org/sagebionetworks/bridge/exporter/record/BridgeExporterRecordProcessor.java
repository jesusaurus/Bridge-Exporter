package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Stopwatch;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.metrics.MetricsHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;

// TODO doc
public class BridgeExporterRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeExporterRecordProcessor.class);

    static final String CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS = "record.loop.delay.millis";
    static final String CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD = "record.loop.progress.report.period";

    private int delayMillis;
    private DynamoHelper dynamoHelper;
    private MetricsHelper metricsHelper;
    private int progressReportPeriod;
    private RecordFilterHelper recordFilterHelper;
    private RecordIdSourceFactory recordIdSourceFactory;

    public final void setConfig(Config config) {
        this.delayMillis = config.getInt(CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS);
        this.progressReportPeriod = config.getInt(CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD);
    }

    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    public final void setMetricsHelper(MetricsHelper metricsHelper) {
        this.metricsHelper = metricsHelper;
    }

    public final void setRecordFilterHelper(RecordFilterHelper recordFilterHelper) {
        this.recordFilterHelper = recordFilterHelper;
    }

    public final void setRecordIdSourceFactory(RecordIdSourceFactory recordIdSourceFactory) {
        this.recordIdSourceFactory = recordIdSourceFactory;
    }

    public void processRecordsForRequest(BridgeExporterRequest request) throws IOException {
        LocalDate requestDate = request.getDate();
        String requestTag = request.getTag();
        LOG.info("Received request with date=" + requestDate + ", tag=" + requestTag);

        Metrics metrics = new Metrics();
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Iterable<String> recordIdIterable = recordIdSourceFactory.getRecordSourceForRequest(request);
            for (String oneRecordId : recordIdIterable) {
                // Count total number of records. Also, log at regular intervals, so people tailing the logs can follow
                // progress.
                int numTotal = metrics.incrementCounter("numTotal");
                if (numTotal % progressReportPeriod == 0) {
                    LOG.info("Num records so far: " + numTotal + " in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                            " seconds");
                }

                // sleep to rate limit our requests to DDB
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException ex) {
                        LOG.error("Record processor interrupted while sleeping: " + ex.getMessage(), ex);
                    }
                }

                try {
                    // get record
                    Item record = dynamoHelper.getRecord(oneRecordId);

                    // filter
                    boolean shouldFilterRecord = recordFilterHelper.shouldFilterRecord(metrics, request, record);
                    if (shouldFilterRecord) {
                        continue;
                    }

                    // only after the filter do we log health code metrics
                    metricsHelper.captureMetricsForRecord(metrics, record);

                    // TODO
                } catch (RuntimeException ex) {
                    LOG.error("Exception processing record " + oneRecordId + ": " + ex.getMessage(), ex);
                }
            }
        } finally {
            LOG.info("Finished processing request in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds, date=" +
                    requestDate + ", tag=" + requestTag);
        }
    }
}
