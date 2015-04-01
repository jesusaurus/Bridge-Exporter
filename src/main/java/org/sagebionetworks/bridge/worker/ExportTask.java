package org.sagebionetworks.bridge.worker;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.exporter.UploadSchemaKey;

/**
 * Represents a single export table task, such as processing a record into TSV row, or uploading the TSV to a Synapse
 * table.
 */
public class ExportTask {
    private final ExportTaskType type;
    private final UploadSchemaKey schemaKey;
    private final Item record;
    private final JsonNode dataJsonNode;

    public ExportTask(ExportTaskType type, UploadSchemaKey schemaKey, Item record, JsonNode dataJsonNode) {
        this.type = type;
        this.schemaKey = schemaKey;
        this.record = record;
        this.dataJsonNode = dataJsonNode;
    }

    /** Task type, such as process record or upload TSV. */
    public ExportTaskType getType() {
        return type;
    }

    /**
     * Schema key this task represents. For PROCESS_RECORD tasks, this represents the schema key in the record. For
     * PROCESS_IOS_SURVEY tasks, this represents the schema of the survey rather than the ios-survey key.
     */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    /** DDB health data record. This is present only for PROCESS_RECORD and PROCESS_IOS_SURVEY tasks. */
    public Item getRecord() {
        return record;
    }

    /**
     * JSON node representing the data normally found in the DDB health data record. This is present only for
     * PROCESS_IOS_SURVEY as iOS surveys need additional processing.
     */
    public JsonNode getDataJsonNode() {
        return dataJsonNode;
    }
}
