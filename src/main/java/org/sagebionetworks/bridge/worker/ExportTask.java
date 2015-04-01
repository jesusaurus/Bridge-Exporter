package org.sagebionetworks.bridge.worker;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a single export table task, such as processing a record into TSV row, or uploading the TSV to a Synapse
 * table.
 */
public class ExportTask {
    private ExportTaskType type;
    private Item record;
    private JsonNode dataJsonNode;

    /** Task type, such as process record or upload TSV. */
    public ExportTaskType getType() {
        return type;
    }

    public void setType(ExportTaskType type) {
        this.type = type;
    }

    /** DDB health data record. This is present only for PROCESS_RECORD and PROCESS_IOS_SURVEY tasks. */
    public Item getRecord() {
        return record;
    }

    public void setRecord(Item record) {
        this.record = record;
    }

    /**
     * JSON node representing the data normally found in the DDB health data record. This is present only for
     * PROCESS_IOS_SURVEY as iOS surveys need additional processing.
     */
    public JsonNode getDataJsonNode() {
        return dataJsonNode;
    }

    public void setDataJsonNode(JsonNode dataJsonNode) {
        this.dataJsonNode = dataJsonNode;
    }
}
