package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.s3.S3Helper;

public class ExportHelperTest {
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String DUMMY_ATTACHMENT_CONTENT = "dummy attachment content";

    @Test
    public void uploadFreeformText() throws Exception {
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // set up export helper with mocks
        Table mockAttachmentsTable = mock(Table.class);
        S3Helper mockS3Helper = mock(S3Helper.class);

        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setDdbAttachmentTable(mockAttachmentsTable);
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
        verify(mockS3Helper).writeBytesToS3(eq(DUMMY_ATTACHMENT_BUCKET), eq(attachmentId),
                attachmentBytesCaptor.capture());

        String attachmentText = new String(attachmentBytesCaptor.getValue(), Charsets.UTF_8);
        assertEquals(attachmentText, DUMMY_ATTACHMENT_CONTENT);
    }
}
