package org.sagebionetworks.bridge.exporter.handler;

import java.util.List;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/** Synapse export worker for tables based on a Bridge schema. */
public class SchemaBasedExportHandler extends HealthDataExportHandler {
    private UploadSchemaKey schemaKey;

    /**
     * Schema that this handler represents. This is used for determining the table keys in DDB as well as determining
     * the Synapse table columns and corresponding TSV columns.
     */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    /** @see #getSchemaKey */
    public final void setSchemaKey(UploadSchemaKey schemaKey) {
        this.schemaKey = schemaKey;
    }

    @Override
    protected String getDdbTableName() {
        return "SynapseTables";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "schemaKey";
    }

    @Override
    protected String getDdbTableKeyValue() {
        return schemaKey.toString();
    }

    @Override
    protected String getSynapseTableName() {
        return schemaKey.getSchemaId() + "-v" + schemaKey.getRevision();
    }

    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return task.getHealthDataTsvInfoForSchema(schemaKey);
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        task.setHealthDataTsvInfoForSchema(schemaKey, tsvInfo);
    }

    @Override
    protected List<UploadFieldDefinition> getSchemaFieldDefList(Metrics metrics) throws SchemaNotFoundException {
        // Gets the field def list from the schema. This calls through to Bridge using the BridgeHelper, which may
        // cache the schema for a few minutes.
        UploadSchema schema = getManager().getBridgeHelper().getSchema(metrics, schemaKey);
        return schema.getFieldDefinitions();
    }
}
