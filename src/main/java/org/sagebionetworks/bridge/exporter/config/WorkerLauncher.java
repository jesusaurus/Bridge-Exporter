package org.sagebionetworks.bridge.exporter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;

/**
 * Launches worker threads. This hooks into the Spring Boot command-line runner, which is really just a big
 * Runnable-equivalent that Spring Boot knows about.
 */
@Component
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    private HeartbeatLogger heartbeatLogger;
    private PollSqsWorker sqsWorker;

    @Autowired
    public final void setHeartbeatLogger(HeartbeatLogger heartbeatLogger) {
        this.heartbeatLogger = heartbeatLogger;
    }

    // This is singleton, because the bottleneck is how many threads we can spin up to handle one export job, not how
    // many export jobs we can do in parallel. If we for some reason find we need to do parallel export jobs, we can
    // move this to multi-threading.
    @Autowired
    public final void setSqsWorker(PollSqsWorker sqsWorker) {
        this.sqsWorker = sqsWorker;
    }

    /**
     * Main entry point into the app. Should only be called by Spring Boot.
     *
     * @param args
     *         command-line args
     */
    @Override
    public void run(String... args) {
        LOG.info("Starting heartbeat...");
        new Thread(heartbeatLogger).start();

        LOG.info("Starting worker...");
        new Thread(sqsWorker).start();
    }
}
