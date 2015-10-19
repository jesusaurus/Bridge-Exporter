package org.sagebionetworks.bridge.exporter.worker;

public class ExportWorker implements Runnable {
    private final ExportHandler handler;
    private final ExportSubtask task;

    public ExportWorker(ExportHandler handler, ExportSubtask task) {
        this.handler = handler;
        this.task = task;
    }

    @Override
    public void run() {
        handler.handle(task);
    }
}
