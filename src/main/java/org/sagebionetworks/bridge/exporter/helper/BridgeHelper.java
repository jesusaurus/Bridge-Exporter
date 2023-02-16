package org.sagebionetworks.bridge.exporter.helper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.jcabi.aspects.Cacheable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.RecordExportStatusRequest;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component
public class BridgeHelper {
    private static final int MAX_BATCH_SIZE = 25;

    private ClientManager bridgeClientManager;

    // Rate limiter, used to limit the amount of traffic to Bridge, specifically for when we loop over a potentially
    // unbounded series of studies. Conservatively limit at 1 req/sec.
    private final RateLimiter rateLimiter = RateLimiter.create(1.0);

    /** Bridge Client Manager, with credentials for Exporter account. This is used to refresh the session. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /** Gets the participant from Bridge for the specified study and health code. */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public StudyParticipant getParticipantByHealthCode(String studyId, String healthCode) {
        try {
            return bridgeClientManager.getClient(ForWorkersApi.class).getParticipantInStudyByHealthCode(studyId,
                    healthCode, false).execute().body();
        } catch (IOException ex) {
            throw new BridgeSDKException("Error getting participant from Bridge: " + ex.getMessage(), ex);
        }
    }

    /**
     * Helper method to update export status in records
     */
    public void updateRecordExporterStatus(List<String> recordIds, SynapseExporterStatus status) {
        // update status
        // breaking down the list into batches whenever the list size exceeds the batch size
        List<List<String>> batches = Lists.partition(recordIds, MAX_BATCH_SIZE);
        batches.forEach(batch-> {
            rateLimiter.acquire();

            RecordExportStatusRequest request = new RecordExportStatusRequest().recordIds(batch).synapseExporterStatus(
                    status);
            try {
                bridgeClientManager.getClient(ForWorkersApi.class).updateRecordExportStatuses(request).execute();
            } catch (IOException ex) {
                throw new BridgeSDKException("Error sending record export statuses to Bridge: " + ex.getMessage(), ex);
            }
        });
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
        try {
            return bridgeClientManager.getClient(ForWorkersApi.class).getSchemaRevisionInStudy(schemaKey.getAppId(),
                    schemaKey.getSchemaId(), (long) schemaKey.getRevision()).execute().body();
        } catch (IOException ex) {
            throw new BridgeSDKException("Error getting schema from Bridge: " + ex.getMessage(), ex);
        }
    }

    /** Calls Bridge to get a study by ID. */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public Study getStudy(String studyId) {
        try {
            return bridgeClientManager.getClient(ForWorkersApi.class).getStudy(studyId).execute().body();
        } catch (IOException ex) {
            throw new BridgeSDKException("Error getting study " + studyId + " from Bridge: " + ex.getMessage(), ex);
        }
    }
}
