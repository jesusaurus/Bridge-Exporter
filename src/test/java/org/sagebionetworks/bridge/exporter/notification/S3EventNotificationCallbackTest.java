package org.sagebionetworks.bridge.exporter.notification;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.amazonaws.services.s3.event.S3EventNotification;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.sagebionetworks.bridge.sdk.WorkerClient;
import org.sagebionetworks.bridge.sdk.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.sdk.exceptions.EntityNotFoundException;

/**
 * Created by liujoshua on 7/12/16.
 */
public class S3EventNotificationCallbackTest {

    private static final String UPLOAD_COMPLETE_MESSAGE="{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\"," +
            "\"awsRegion\":\"us-east-1\"," +
            "\"eventTime\":\"2016-07-12T22:06:54.454Z\",\"eventName\":\"ObjectCreated:Put\"," +
            "\"userIdentity\":{\"principalId\":\"AWS:AIDAJCSQZ35H7B4BFOVAW\"},\"requestParameters\":{\"sourceIPAddress\":\"54.87.180" +
            ".29\"},\"responseElements\":{\"x-amz-request-id\":\"006B5645F94646A3\"," +
            "\"x-amz-id-2\":\"wk7j6of4ftpRy+lbjt4olcNqp8S2s9d7XUbdOv4UEDh7B+8myhMe45xBEZPUP4+5oxwY9r2z9Yw=\"}," +
            "\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Bridge Upload Complete Notification UAT\"," +
            "\"bucket\":{\"name\":\"org-sagebridge-upload-uat\",\"ownerIdentity\":{\"principalId\":\"AZ9HQM5UC903F\"}," +
            "\"arn\":\"arn:aws:s3:::org-sagebridge-upload-uat\"},\"object\":{\"key\":\"89b40dab-4982-4d5c-ae21-d74b072d02cd\"," +
            "\"size\":1488,\"eTag\":\"e40df5cfa5874ab353947eb48ec0cfa4\",\"sequencer\":\"00578569FE6792370C\"}}}]}";
    private static final String UPLOAD_ID = "89b40dab-4982-4d5c-ae21-d74b072d02cd";

    private WorkerClient mockWorkerClient;
    private S3EventNotificationCallback callback;


    @BeforeMethod
    public void before() {
        mockWorkerClient = mock(WorkerClient.class);

        callback = spy(new S3EventNotificationCallback());
        doReturn(mockWorkerClient).when(callback).getWorkerClient();
    }

    @Test
    public void testCallback_StringMessage() throws Exception {
        callback.callback(UPLOAD_COMPLETE_MESSAGE);

        verify(mockWorkerClient, times(1)).completeUpload(UPLOAD_ID);
    }

    @Test
    public void testCallback_NullList() throws Exception {
        callback.callback("{}");
        verify(mockWorkerClient, never()).completeUpload(anyString());
    }

    @Test
    public void testCallback_EmptyList() throws Exception {
        callback.callback("{\"Records\":[]}");
        verify(mockWorkerClient, never()).completeUpload(anyString());
    }

    @Test
    public void testCallback_Throws500Exceptions() throws Exception {
        doThrow(new BridgeSDKException("internal error", 500)).when(mockWorkerClient).completeUpload(UPLOAD_ID);

        try {
            callback.callback(UPLOAD_COMPLETE_MESSAGE);
            fail("expected exception");
        } catch (RuntimeException ex) {
            BridgeSDKException innerEx = (BridgeSDKException) ex.getCause();
            assertEquals(500, innerEx.getStatusCode());
        }
    }

    @Test
    public void testCallback_Throws404Exceptions() throws Exception {
        doThrow(new EntityNotFoundException("not found", "dummy endpoint")).when(mockWorkerClient)
                .completeUpload(UPLOAD_ID);
        callback.callback(UPLOAD_COMPLETE_MESSAGE);
        verify(mockWorkerClient, times(1)).completeUpload(UPLOAD_ID);
    }

    @Test
    public void testCallback_CompleteUploadForShouldProcessRecords() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        S3EventNotification.S3EventNotificationRecord record1 = createMockRecordWithKey(key1);
        S3EventNotification.S3EventNotificationRecord record2 = createMockRecordWithKey(key2);
        S3EventNotification.S3EventNotificationRecord record3 = createMockRecordWithKey(key3);
        record1.getS3().getObject().getKey();

        S3EventNotification notification = mock(S3EventNotification.class);
        when(notification.getRecords()).thenReturn(Lists.newArrayList(record1, record2, record3));

        doReturn(true).when(callback).shouldProcessRecord(record1);
        doReturn(false).when(callback).shouldProcessRecord(record2);
        doReturn(true).when(callback).shouldProcessRecord(record3);

        callback.callback(notification);

        verify(mockWorkerClient, times(1)).completeUpload(key1);
        verify(mockWorkerClient, times(1)).completeUpload(key3);
    }

    private S3EventNotification.S3EventNotificationRecord createMockRecordWithKey(String key) {
        S3EventNotification.S3ObjectEntity object = mock(S3EventNotification.S3ObjectEntity.class);
        when(object.getKey()).thenReturn(key);

        S3EventNotification.S3Entity entity = mock(S3EventNotification.S3Entity.class);
        when(entity.getObject()).thenReturn(object);

        S3EventNotification.S3EventNotificationRecord record = mock(S3EventNotification.S3EventNotificationRecord.class);
        when(record.getS3()).thenReturn(entity);

        return record;
    }

    @Test
    public void testShouldProcessRecords_S3Put() {
        S3EventNotification.S3EventNotificationRecord s3Put = createMockRecord("aws:s3", "ObjectCreated:Put");

        assertTrue(callback.shouldProcessRecord(s3Put));
    }

    @Test
    public void testShouldProcessRecords_S3CompleteMultipartUpload() {
        S3EventNotification.S3EventNotificationRecord s3Put = createMockRecord("aws:s3", "ObjectCreated:CompleteMultipartUpload");

        assertTrue(callback.shouldProcessRecord(s3Put));
    }

    @Test
    public void testShouldProcessRecords_S3Post() {
        S3EventNotification.S3EventNotificationRecord s3Put = createMockRecord("aws:s3", "ObjectCreated:Post");

        assertTrue(callback.shouldProcessRecord(s3Put));
    }

    @Test
    public void testShouldProcessRecords_S3Delete() {
        S3EventNotification.S3EventNotificationRecord s3Delete = createMockRecord("aws:s3", "ObjectRemoved:Delete");

        assertFalse(callback.shouldProcessRecord(s3Delete));
    }

    @Test
    public void testShouldProcessRecords_NotS3() {
        S3EventNotification.S3EventNotificationRecord notS3 = createMockRecord("aws:dynamo", "ObjectRemoved:Delete");

        assertFalse(callback.shouldProcessRecord(notS3));
    }

    private S3EventNotification.S3EventNotificationRecord createMockRecord(String source, String name) {
        S3EventNotification.S3EventNotificationRecord record = mock(S3EventNotification.S3EventNotificationRecord.class);
        when(record.getEventSource()).thenReturn(source);
        when(record.getEventName()).thenReturn(name);

        return record;
    }
}
