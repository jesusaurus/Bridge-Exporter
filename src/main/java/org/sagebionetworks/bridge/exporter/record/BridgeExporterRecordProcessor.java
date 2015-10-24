package org.sagebionetworks.bridge.exporter.record;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.metrics.MetricsHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.FileHelper;

// TODO doc
@Component
public class BridgeExporterRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeExporterRecordProcessor.class);

    static final String CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS = "record.loop.delay.millis";
    static final String CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD = "record.loop.progress.report.period";
    static final String CONFIG_KEY_TIME_ZONE_NAME = "time.zone.name";

    // config attributes
    private int delayMillis;
    private int progressReportPeriod;
    private DateTimeZone timeZone;

    // Spring helpers
    private Table ddbRecordTable;
    private FileHelper fileHelper;
    private MetricsHelper metricsHelper;
    private RecordFilterHelper recordFilterHelper;
    private RecordIdSourceFactory recordIdSourceFactory;
    private ExportWorkerManager workerManager;

    @Autowired
    public final void setConfig(Config config) {
        this.delayMillis = config.getInt(CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS);
        this.progressReportPeriod = config.getInt(CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD);
        this.timeZone = DateTimeZone.forID(config.get(CONFIG_KEY_TIME_ZONE_NAME));
    }

    @Resource(name = "ddbRecordTable")
    public final void setDdbRecordTable(Table ddbRecordTable) {
        this.ddbRecordTable = ddbRecordTable;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setMetricsHelper(MetricsHelper metricsHelper) {
        this.metricsHelper = metricsHelper;
    }

    @Autowired
    public final void setRecordFilterHelper(RecordFilterHelper recordFilterHelper) {
        this.recordFilterHelper = recordFilterHelper;
    }

    @Autowired
    public final void setRecordIdSourceFactory(RecordIdSourceFactory recordIdSourceFactory) {
        this.recordIdSourceFactory = recordIdSourceFactory;
    }

    @Autowired
    public final void setWorkerManager(ExportWorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    public void processRecordsForRequest(BridgeExporterRequest request) throws IOException {
        LocalDate requestDate = request.getDate();
        String requestTag = request.getTag();
        LOG.info("Received request with date=" + requestDate + ", tag=" + requestTag);

        // make task
        Metrics metrics = new Metrics();
        File tmpDir = fileHelper.createTempDir();
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.now(timeZone)).withMetrics(metrics)
                .withRequest(request).withTmpDir(tmpDir).build();

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
                    Item record = ddbRecordTable.getItem("recordId", oneRecordId);

                    // filter
                    boolean shouldFilterRecord = recordFilterHelper.shouldFilterRecord(metrics, request, record);
                    if (shouldFilterRecord) {
                        continue;
                    }

                    // only after the filter do we log health code metrics
                    metricsHelper.captureMetricsForRecord(metrics, record);

                    workerManager.addSubtaskForRecord(task, record);
                } catch (BridgeExporterException | RuntimeException | SchemaNotFoundException ex) {
                    LOG.error("Exception processing record " + oneRecordId + ": " + ex.getMessage(), ex);
                }
            }

            workerManager.endOfStream(task);
        } finally {
            LOG.info("Finished processing request in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds, date=" +
                    requestDate + ", tag=" + requestTag);
            metricsHelper.publishMetrics(metrics);
        }

        // cleanup
        fileHelper.deleteDir(tmpDir);
    }
}
