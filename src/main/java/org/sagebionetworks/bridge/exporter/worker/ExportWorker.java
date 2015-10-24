package org.sagebionetworks.bridge.exporter.worker;

import org.sagebionetworks.bridge.exporter.handler.ExportHandler;

public class ExportWorker implements Runnable {
    private final ExportHandler handler;
    private final ExportSubtask subtask;

    public ExportWorker(ExportHandler handler, ExportSubtask subtask) {
        this.handler = handler;
        this.subtask = subtask;
    }

    @Override
    public void run() {
        handler.handle(subtask);
    }
}
