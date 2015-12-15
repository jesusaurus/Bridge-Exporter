package org.sagebionetworks.bridge.exporter.worker;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * Represents a single subtask for a single handler. This generally corresponds one-to-one with a single health data
 * record. Subtasks are immutable, so it's safe to re-use the same subtask for multiple handlers.
 */
public class ExportSubtask {
    private final Item originalRecord;
    private final ExportTask parentTask;
    private final JsonNode recordData;
    private final UploadSchemaKey schemaKey;

    // Private constructor. To build, use Builder.
    private ExportSubtask(Item originalRecord, ExportTask parentTask, JsonNode recordData, UploadSchemaKey schemaKey) {
        this.originalRecord = originalRecord;
        this.parentTask = parentTask;
        this.recordData = recordData;
        this.schemaKey = schemaKey;
    }

    /** DDB health data record. */
    public Item getOriginalRecord() {
        return originalRecord;
    }

    /** Parent exporter task. */
    public ExportTask getParentTask() {
        return parentTask;
    }

    /**
     * JSON node representing the data normally found in the DDB health data record. This exists because legacy iOS
     * surveys wrote their answers to an attachment instead of directly to the health data record. All handlers should
     * consume health data from getRecordData() instead of from getOriginalRecord().
     */
    public JsonNode getRecordData() {
        return recordData;
    }

    /**
     * Schema key this task represents, used for Health Data and App Version tasks. (Legacy iOS survey tasks have the
     * schema ios-survey instead of the real survey.)
     */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    /** Subtask builder. */
    public static class Builder {
        private Item originalRecord;
        private ExportTask parentTask;
        private JsonNode recordData;
        private UploadSchemaKey schemaKey;

        /** @see ExportSubtask#getOriginalRecord */
        public Builder withOriginalRecord(Item originalRecord) {
            this.originalRecord = originalRecord;
            return this;
        }

        /** @see ExportSubtask#getParentTask */
        public Builder withParentTask(ExportTask parentTask) {
            this.parentTask = parentTask;
            return this;
        }

        /** @see ExportSubtask#getRecordData */
        public Builder withRecordData(JsonNode recordData) {
            this.recordData = recordData;
            return this;
        }

        /** @see ExportSubtask#getSchemaKey */
        public Builder withSchemaKey(UploadSchemaKey schemaKey) {
            this.schemaKey = schemaKey;
            return this;
        }

        /** Builds an ExportSubtask object and validates that all fields are valid (that is, non-null). */
        public ExportSubtask build() {
            // validate - all fields must be non-null
            if (originalRecord == null) {
                throw new IllegalStateException("originalRecord must be non-null");
            }

            if (parentTask == null) {
                throw new IllegalStateException("parentTask must be non-null");
            }

            if (recordData == null) {
                throw new IllegalStateException("recordData must be non-null");
            }

            if (schemaKey == null) {
                throw new IllegalStateException("schemaKey must be non-null");
            }

            return new ExportSubtask(originalRecord, parentTask, recordData, schemaKey);
        }
    }
}
