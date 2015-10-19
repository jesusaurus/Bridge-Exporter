package org.sagebionetworks.bridge.exporter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Launches worker threads. This hooks into the Spring Boot command-line runner, which is really just a big
 * Runnable-equivalent that Spring Boot knows about.
 */
@Component
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    /**
     * Main entry point into the app. Should only be called by Spring Boot.
     *
     * @param args
     *         command-line args
     */
    @Override
    public void run(String... args) {
        LOG.info("Hello world!");
    }
}
