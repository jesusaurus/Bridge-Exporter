package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Charsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class ExportHelperTest {
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String DUMMY_ATTACHMENT_CONTENT = "dummy attachment content";

    private static final byte[] MOCK_MD5 = { -104, 10, -30, -37, 25, -113, 92, -9, 69, -118, -46, -87, 11, -14, 38, -61 };
    private static final String MOCK_MD5_HEX_ENCODED = "980ae2db198f5cf7458ad2a90bf226c3";

    private static final String UPLOAD_END_DATE_TIME = "2016-05-09T23:59:59.999-0700";

    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);

    @BeforeClass
    public void mockTime() {
        DateTimeUtils.setCurrentMillisFixed(UPLOAD_END_DATE_TIME_OBJ.getMillis());
    }

    @AfterClass
    public void cleanupTime() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void uploadFreeformText() throws Exception {
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // set up export helper with mocks
        DigestUtils mockMd5DigestUtils = mock(DigestUtils.class);
        when(mockMd5DigestUtils.digest(any(byte[].class))).thenReturn(MOCK_MD5);

        Table mockAttachmentsTable = mock(Table.class);
        S3Helper mockS3Helper = mock(S3Helper.class);

        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setDdbAttachmentTable(mockAttachmentsTable);
        helper.setMd5DigestUtils(mockMd5DigestUtils);
        helper.setS3Helper(mockS3Helper);

        // execute
        String attachmentId = helper.uploadFreeformTextAsAttachment(DUMMY_RECORD_ID, DUMMY_ATTACHMENT_CONTENT);

        // Attachment ID is randomly generated. Assert that it's not blank and assert that it's the same as what we
        // write into DDB and S3.
        assertTrue(StringUtils.isNotBlank(attachmentId));

        // verify DDB attachment
        ArgumentCaptor<Item> attachmentItemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockAttachmentsTable).putItem(attachmentItemCaptor.capture());

        Item attachmentItem = attachmentItemCaptor.getValue();
        assertEquals(attachmentItem.getString("id"), attachmentId);
        assertEquals(attachmentItem.getString("recordId"), DUMMY_RECORD_ID);

        // verify S3 attachment
        ArgumentCaptor<byte[]> attachmentBytesCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeBytesToS3(eq(DUMMY_ATTACHMENT_BUCKET), eq(attachmentId),
                attachmentBytesCaptor.capture(), metadataCaptor.capture());

        String attachmentText = new String(attachmentBytesCaptor.getValue(), Charsets.UTF_8);
        assertEquals(attachmentText, DUMMY_ATTACHMENT_CONTENT);

        ObjectMetadata metadata = metadataCaptor.getValue();
        assertEquals(metadata.getUserMetaDataOf(BridgeExporterUtil.KEY_CUSTOM_CONTENT_MD5), MOCK_MD5_HEX_ENCODED);
        assertEquals(metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }
}
