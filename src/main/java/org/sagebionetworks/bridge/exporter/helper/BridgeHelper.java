package org.sagebionetworks.bridge.exporter.helper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.jcabi.aspects.Cacheable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.rest.model.RecordExportStatusRequest;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);
    private static final int MAX_BATCH_SIZE = 25;

    private ClientManager bridgeClientManager;
    private SignIn bridgeCredentials;

    /** Bridge Client Manager, with credentials for Exporter account. This is used to refresh the session. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /** Bridge credentials, used by the session helper to refresh the session. */
    @Autowired
    public final void setBridgeCredentials(SignIn bridgeCredentials) {
        this.bridgeCredentials = bridgeCredentials;
    }

    /**
     * Signals Bridge Server that the upload is completed and to begin processing the upload. Used by Upload
     * Auto-Complete.
     *
     * @param uploadId
     *         upload to mark completed and begin processing
     */
    public void completeUpload(String uploadId) {
        try {
            sessionHelper(() -> bridgeClientManager.getClient(ForWorkersApi.class).completeUploadSession(uploadId)
                    .execute());
        } catch (IOException ex) {
            throw new BridgeSDKException("Error completing upload to Bridge: " + ex.getMessage(), ex);
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
            RecordExportStatusRequest request = new RecordExportStatusRequest().recordIds(batch).synapseExporterStatus(
                    status);
            try {
                sessionHelper(() -> bridgeClientManager.getClient(ForWorkersApi.class)
                        .updateRecordExportStatuses(request).execute());
            } catch (IOException ex) {
                throw new BridgeSDKException("Error sending record export statuses to Bridge: " + ex.getMessage(), ex);
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.warn("Error sleeping while sending export statuses: " + e.getMessage(), e);
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
            return sessionHelper(() -> bridgeClientManager.getClient(ForWorkersApi.class).getSchemaRevisionInStudy(
                    schemaKey.getStudyId(), schemaKey.getSchemaId(), (long) schemaKey.getRevision()).execute().body());
        } catch (IOException ex) {
            throw new BridgeSDKException("Error getting schema from Bridge: " + ex.getMessage(), ex);
        }
    }

    // Helper method, which wraps a Bridge Server call with logic for initializing and refreshing a session.
    private <T> T sessionHelper(BridgeCallable<T> callable) throws IOException {
        // First attempt. This should be enough for most cases.
        try {
            return callable.call();
        } catch (NotAuthenticatedException ex) {
            // Code readability reasons, the error handling will be done after the catch block instead of inside the
            // catch block.
        }

        // Refresh session and try again. This time, if the call fails, just let the exception bubble up.
        LOG.info("Bridge server session expired. Refreshing session...");
        bridgeClientManager.getClient(AuthenticationApi.class).signIn(bridgeCredentials).execute();

        return callable.call();
    }

    // Functional interface used to make lambdas for the session helper.
    @FunctionalInterface
    interface BridgeCallable<T> {
        T call() throws IOException;
    }
}
