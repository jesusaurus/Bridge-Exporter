package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.helper.ExportHelper.IDENTIFIER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class ExportHelperTest {
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String DUMMY_ATTACHMENT_CONTENT = "dummy attachment content";

    private static final String UPLOAD_DATE = "2016-05-09";
    private static final String UPLOAD_START_DATE_TIME = "2016-05-09T00:00:00.000-0700";
    private static final String UPLOAD_END_DATE_TIME = "2016-05-09T23:59:59.999-0700";
    private static final String EXPORT_TIME_TABLE_NAME = "exportTime";
    private static final String STUDY_TABLE_NAME = "Study";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);
    private static final LocalDate UPLOAD_DATE_OBJ = LocalDate.parse(UPLOAD_DATE);

    @Test
    public void uploadFreeformText() throws Exception {
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

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

    @Test
    public void getEndDateTimeTest() {
        BridgeExporterRequest request;
        DateTime endDateTime;
        ExportHelper exportHelper = new ExportHelper();

        // DAILY
        request = new BridgeExporterRequest.Builder().withDate(UPLOAD_DATE_OBJ).build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertEquals(endDateTime.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());

        // HOURLY
        request = new BridgeExporterRequest.Builder().withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withStudyWhitelist(ImmutableSet.of("dummy-whitelist"))
                .build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertEquals(endDateTime.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());

        // s3 override
        request = new BridgeExporterRequest.Builder().withRecordIdS3Override("dummy-override").build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertNull(endDateTime);
    }

    @Test
    public void bootstrapStudyIdsToQueryTest() {
        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        // mock study table and study id list
        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getTableName()).thenReturn(STUDY_TABLE_NAME);

        List<Map<String, AttributeValue>> studyIdList = ImmutableList.of(
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-foo")),
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-bar"))
        );

        ScanResult scanResult = new ScanResult();
        scanResult.setItems(studyIdList);
        ScanRequest scanRequest = new ScanRequest().withTableName(STUDY_TABLE_NAME);
        when(mockDdbClient.scan(eq(scanRequest))).thenReturn(scanResult);

        ExportHelper exportHelper = new ExportHelper();
        exportHelper.setDdbClientScan(mockDdbClient);
        exportHelper.setDdbStudyTable(mockStudyTable);

        // mock request
        BridgeExporterRequest request;
        List<String> studyIdsToUpdate;
        // daily
        request = new BridgeExporterRequest.Builder().withDate(UPLOAD_DATE_OBJ).build();
        studyIdsToUpdate = exportHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get(0), studyIdList.get(0).get(IDENTIFIER).getS());
        assertEquals(studyIdsToUpdate.get(1), studyIdList.get(1).get(IDENTIFIER).getS());

        // with whitelist
        request = new BridgeExporterRequest.Builder().withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withStudyWhitelist(ImmutableSet.of("ddb-foo"))
                .build();

        studyIdsToUpdate = exportHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 1);
        assertEquals(studyIdsToUpdate.get(0), "ddb-foo");
    }
}
