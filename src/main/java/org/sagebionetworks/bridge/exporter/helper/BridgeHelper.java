package org.sagebionetworks.bridge.exporter.helper;

import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.Cacheable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.WorkerClient;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component
public class BridgeHelper {
    private WorkerClient workerClient;

    /** Bridge Client for worker APIs. */
    @Autowired
    final void setWorkerClient(WorkerClient workerClient) {
        this.workerClient = workerClient;
    }

    /**
     * Returns the schema for the given key.
     *
     * @param metrics
     *         metrics object, used to keep a record of "schemas not found"
     * @param schemaKey
     *         key for the schema to get
     * @return the schema
     * @throws SchemaNotFoundException
     *         if the schema doesn't exist
     */
    public UploadSchema getSchema(Metrics metrics, UploadSchemaKey schemaKey) throws SchemaNotFoundException {
        UploadSchema schema = getSchemaCached(schemaKey);
        if (schema == null) {
            metrics.addKeyValuePair("schemasNotFound", schemaKey.toString());
            throw new SchemaNotFoundException("Schema not found: " + schemaKey.toString());
        }
        return schema;
    }

    // Helper method that encapsulates just the service call, cached with annotation.
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    private UploadSchema getSchemaCached(UploadSchemaKey schemaKey) {
        return workerClient.getSchema(schemaKey.getStudyId(), schemaKey.getSchemaId(), schemaKey.getRevision());
    }
}
