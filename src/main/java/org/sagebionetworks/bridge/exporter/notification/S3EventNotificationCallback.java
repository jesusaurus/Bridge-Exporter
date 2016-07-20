package org.sagebionetworks.bridge.exporter.notification;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.event.S3EventNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcabi.aspects.Cacheable;

import org.sagebionetworks.bridge.sdk.ClientProvider;
import org.sagebionetworks.bridge.sdk.Session;
import org.sagebionetworks.bridge.sdk.WorkerClient;
import org.sagebionetworks.bridge.sdk.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;

/**
 * Created by liujoshua on 7/12/16.
 */
@Component
public class S3EventNotificationCallback implements PollSqsCallback {
    private static final Logger LOG = LoggerFactory.getLogger(S3EventNotificationCallback.class);
    private static final String S3_EVENT_SOURCE = "aws:s3";
    private static final String S3_OBJECT_CREATED_EVENT_PREFIX = "ObjectCreated:";

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private SignInCredentials credentials;

    @Autowired
    final void setCredentials(SignInCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Helper method to encapsulate refreshing the Bridge session. This uses a cache to cache the session for 5
     * minutes. Package-scoped to enable unit tests to mock this out.
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    WorkerClient getWorkerClient() {
        Session session = ClientProvider.signIn(credentials);
        return session.getWorkerClient();
    }

    @Override
    public void callback(String messageBody) throws Exception {
        S3EventNotification notification = OBJECT_MAPPER.readValue(messageBody, S3EventNotification.class);
        List<S3EventNotification.S3EventNotificationRecord> recordList = notification.getRecords();
        if (recordList == null || recordList.isEmpty()) {
            // Notification w/o record list is not actionable. Log a warning and squelch.
            LOG.warn("S3 notification without record list: " + messageBody);
            return;
        }

        callback(notification);
    }

    // package-scoped to enable mocking/testing
    void callback(S3EventNotification notification) {
        notification.getRecords().stream().filter(record -> shouldProcessRecord(record)).forEach(record -> {
            String uploadId = record.getS3().getObject().getKey();

            try {
                getWorkerClient().completeUpload(uploadId);
                LOG.info("Completed upload, id=" + uploadId);
            } catch (BridgeSDKException ex) {
                String errorMsg = "Error completing upload id " + uploadId + ": " + ex.getMessage();
                int status = ex.getStatusCode();
                if (400 <= status && status <= 499) {
                    // HTTP 4XX means bad request (such as 404 not found). This can happen for a variety of reasons and
                    // is generally not developer actionable. Log a warning and swallow the exception. This way, the
                    // SQS poll worker will succeed the callback and delete the message, preventing spurious retries.
                    LOG.warn(errorMsg, ex);
                } else {
                    // A non-4XX error generally means a server error. We'll want to retry this. Log an error and
                    // re-throw.
                    LOG.error(errorMsg, ex);

                    // Foreach handlers can't throw checked exceptions. It's not worth creating an unchecked exception
                    // given that we're about to refactor error handling. For now, just throw a RuntimeException.
                    throw new RuntimeException(errorMsg, ex);
                }
            }
        });
    }

    // package-scoped to enable mocking/testing
    boolean shouldProcessRecord(S3EventNotification.S3EventNotificationRecord record) {
        return S3_EVENT_SOURCE.equals(record.getEventSource()) && record.getEventName().startsWith(S3_OBJECT_CREATED_EVENT_PREFIX);
    }
}
