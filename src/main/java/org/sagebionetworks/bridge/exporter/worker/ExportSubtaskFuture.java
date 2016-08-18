package org.sagebionetworks.bridge.exporter.worker;

import java.util.concurrent.Future;

/**
 * Wrapper class that encapsulates both the ExportSubtask and its associated asynchronous Future. This is used to track
 * remaining subtask executions and to tie Futures back to the subtask.
 */
public class ExportSubtaskFuture {
    private final Future<?> future;
    private final ExportSubtask subtask;

    /** Private constructor. To construct, use builder. */
    private ExportSubtaskFuture(Future<?> future, ExportSubtask subtask) {
        this.future = future;
        this.subtask = subtask;
    }

    /** Future holding the asynchronous execution of the subtask. */
    public Future<?> getFuture() {
        return future;
    }

    /** Export subtask, generally representing one health data record. */
    public ExportSubtask getSubtask() {
        return subtask;
    }

    /** Builder. */
    public static class Builder {
        private Future<?> future;
        private ExportSubtask subtask;

        /** @see #getFuture */
        public Builder withFuture(Future<?> future) {
            this.future = future;
            return this;
        }

        /** @see #getSubtask */
        public Builder withSubtask(ExportSubtask subtask) {
            this.subtask = subtask;
            return this;
        }

        /** Validates that all params have been set and builds an ExportSubtaskFuture. */
        public ExportSubtaskFuture build() {
            if (future == null) {
                throw new IllegalStateException("future must be specified");
            }
            if (subtask == null) {
                throw new IllegalStateException("subtask must be specified");
            }

            return new ExportSubtaskFuture(future, subtask);
        }
    }
}
