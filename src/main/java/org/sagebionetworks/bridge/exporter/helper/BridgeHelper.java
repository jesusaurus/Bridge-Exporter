package org.sagebionetworks.bridge.exporter.helper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.Cacheable;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.sdk.models.healthData.RecordExportStatusRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.ClientProvider;
import org.sagebionetworks.bridge.sdk.Session;
import org.sagebionetworks.bridge.sdk.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    private SignInCredentials credentials;
    private Session session = null;

    /** Bridge credentials for the Exporter account. This needs to be saved in memory so we can refresh the session. */
    @Autowired
    final void setCredentials(SignInCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Signals Bridge Server that the upload is completed and to begin processing the upload. Used by Upload
     * Auto-Complete.
     *
     * @param uploadId
     *         upload to mark completed and begin processing
     */
    public void completeUpload(String uploadId) {
        sessionHelper(() -> {
            session.getWorkerClient().completeUpload(uploadId);
            // Needs return null because session helper expects a return value.
            return null;
        });
    }

    /**
     * Helper method to update export status in records
     */
    public void updateRecordExporterStatus(List<String> recordIds, RecordExportStatusRequest.ExporterStatus status) {
        // first create a credentials for this session
        SpringConfig config = new SpringConfig();
        SignInCredentials credentials = config.bridgeWorkerCredentials();
        setCredentials(credentials);

        // then update status
        RecordExportStatusRequest request = new RecordExportStatusRequest(recordIds, status);
        sessionHelper(() -> {
            session.getWorkerClient().updateRecordExporterStatus(request);
            return null;
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
        return sessionHelper(() -> session.getUploadSchemaClient().getSchema(schemaKey.getStudyId(),
                schemaKey.getSchemaId(), schemaKey.getRevision()));
    }

    // Helper method, which wraps a Bridge Server call with logic for initializing and refreshing a session.
    private <T> T sessionHelper(BridgeCallable<T> callable) {
        // Init session if necessary.
        if (session == null) {
            session = signIn();
        }

        // First attempt. This should be enough for most cases.
        try {
            return callable.call();
        } catch (NotAuthenticatedException ex) {
            // Code readability reasons, the error handling will be done after the catch block instead of inside the
            // catch block.
        }

        // Refresh session and try again. This time, if the call fails, just let the exception bubble up.
        LOG.info("Bridge server session expired. Refreshing session...");
        session = signIn();
        return callable.call();
    }

    // Helper method to sign in to Bridge Server and get a session. This needs to be wrapped because the sign-in call
    // is static and therefore not mockable.
    // Package-scoped to facilitate unit tests.
    Session signIn() {
        return ClientProvider.signIn(credentials);
    }

    // Functional interface used to make lambdas for the session helper.
    @FunctionalInterface
    interface BridgeCallable<T> {
        T call();
    }
}
