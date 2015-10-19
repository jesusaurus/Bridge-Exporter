package org.sagebionetworks.bridge.exporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class UploadSchemaHelper {
    // Externally configured.
    private BridgeExporterConfig config;
    private DynamoDB ddbClient;

    // Internal state.
    private final Map<UploadSchemaKey, UploadSchema> schemaMap = new HashMap<>();
    private final Set<String> schemasNotFound = new HashSet<>();

    /** Bridge Exporter configuration. Configured externally. */
    public BridgeExporterConfig getConfig() {
        return config;
    }

    public void setConfig(BridgeExporterConfig config) {
        this.config = config;
    }

    /** DynamoDB client. Externally configured. */
    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** Returns a set of schemas that were queried but not found. */
    public Set<String> getSchemasNotFound() {
        return ImmutableSet.copyOf(schemasNotFound);
    }

    public void init() throws IOException {
        // schemas
        Table uploadSchemaTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "UploadSchema");
        Iterable<Item> schemaIter = uploadSchemaTable.scan();
        for (Item oneDdbSchema : schemaIter) {
            UploadSchema schema = UploadSchema.fromDdbItem(oneDdbSchema);
            schemaMap.put(schema.getKey(), schema);
        }
    }

    public Set<UploadSchemaKey> getAllSchemaKeys() {
        return schemaMap.keySet();
    }

    public UploadSchema getSchema(UploadSchemaKey schemaKey) throws SchemaNotFoundException {
        UploadSchema schema = schemaMap.get(schemaKey);
        if (schema == null) {
            schemasNotFound.add(schemaKey.toString());
            throw new SchemaNotFoundException("Schema " + schemaKey.toString() + " not found");
        }
        return schema;
    }
}
