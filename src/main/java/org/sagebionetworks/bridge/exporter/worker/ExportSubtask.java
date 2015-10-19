package org.sagebionetworks.bridge.exporter.worker;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO re-doc
/**
 * Represents a single export table task, such as processing a record into TSV row, or uploading the TSV to a Synapse
 * table.
 */
public class ExportSubtask {
    private final Item originalRecord;
    private final ExportTask parentTask;
    private final JsonNode recordData;
    private final UploadSchemaKey schemaKey;

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

    public ExportTask getParentTask() {
        return parentTask;
    }

    /**
     * JSON node representing the data normally found in the DDB health data record. This is used by the iOS Survey
     * Export Handler to transform the iOS Survey into something the Health Data Export Handler can use. The Health
     * Data Export Handler should look at this node before falling back to the record.
     */
    public JsonNode getRecordData() {
        return recordData;
    }

    /**
     * Schema key this task represents, used for Health Data and App Version tasks, but not iOS Survey tasks (since iOS
     * Survey tasks don't know the real schema). */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    public static class Builder {
        private Item originalRecord;
        private ExportTask parentTask;
        private JsonNode recordData;
        private UploadSchemaKey schemaKey;

        public Builder withOriginalRecord(Item originalRecord) {
            this.originalRecord = originalRecord;
            return this;
        }

        public Builder withParentTask(ExportTask parentTask) {
            this.parentTask = parentTask;
            return this;
        }

        public Builder withRecordData(JsonNode recordData) {
            this.recordData = recordData;
            return this;
        }

        public Builder withSchemaKey(UploadSchemaKey schemaKey) {
            this.schemaKey = schemaKey;
            return this;
        }

        public ExportSubtask build() {
            // TODO validate
            return new ExportSubtask(originalRecord, parentTask, recordData, schemaKey);
        }
    }
}
