package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.STUDY_ID;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyConditions;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class RecordIdSourceFactoryTest {
    private static final String FOO_LAST_EXPORT_TIME_STRING = "2016-05-09T20:25:31.346-0700";
    private static final DateTime FOO_LAST_EXPORT_TIME = DateTime.parse(FOO_LAST_EXPORT_TIME_STRING);

    private static final String BAR_LAST_EXPORT_TIME_STRING = "2016-05-09T13:32:46.695-0700";
    private static final DateTime BAR_LAST_EXPORT_TIME = DateTime.parse(BAR_LAST_EXPORT_TIME_STRING);

    private static final String END_DATE_TIME_STRING = "2016-05-09T23:37:44.326-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STRING);

    @Test
    public void fromDdbNormal() throws Exception {
        // mock map
        Map<String, DateTime> studyIdsToQuery = ImmutableMap.<String, DateTime>builder()
                .put("ddb-foo", FOO_LAST_EXPORT_TIME).put("ddb-bar", BAR_LAST_EXPORT_TIME).build();

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);

        ArgumentCaptor<RangeKeyCondition> barRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);

        // mock DDB
        List<Item> fooStudyItemList = ImmutableList.of(new Item().withString("id", "foo-1"),
                new Item().withString("id", "foo-2"));
        List<Item> barStudyItemList = ImmutableList.of(new Item().withString("id", "bar-1"),
                new Item().withString("id", "bar-2"));

        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-bar"), barRangeKeyCaptor.capture()))
                .thenReturn(barStudyItemList);

        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-foo"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setConfig(mockConfig());
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withUseLastExportTime(true).build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request, studyIdsToQuery);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 4); // only output records in given time range
        assertEquals(recordIdList.get(0), "foo-1");
        assertEquals(recordIdList.get(1), "foo-2");
        assertEquals(recordIdList.get(2), "bar-1");
        assertEquals(recordIdList.get(3), "bar-2");

        validateRangeKey(fooRangeKeyCaptor.getValue(), FOO_LAST_EXPORT_TIME.getMillis(), END_DATE_TIME.getMillis());
        validateRangeKey(barRangeKeyCaptor.getValue(), BAR_LAST_EXPORT_TIME.getMillis(), END_DATE_TIME.getMillis());
    }

    private static void validateRangeKey(RangeKeyCondition rangeKey, long expectedStartMillis,
            long expectedEndMillis) {
        assertEquals(rangeKey.getAttrName(), "uploadedOn");
        assertEquals(rangeKey.getKeyCondition(), KeyConditions.BETWEEN);

        Object[] rangeKeyValueArray = rangeKey.getValues();
        assertEquals(rangeKeyValueArray.length, 2);
        assertEquals(rangeKeyValueArray[0], expectedStartMillis);

        // end date is exclusive, so the query is actually 1 millisecond before
        assertEquals(rangeKeyValueArray[1], expectedEndMillis - 1);
    }

    @Test
    public void fromS3Override() throws Exception {
        // mock S3
        List<String> s3Lines = ImmutableList.of("s3-foo", "s3-bar", "s3-baz");
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsLines("dummy-override-bucket", "dummy-override-file")).thenReturn(s3Lines);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setConfig(mockConfig());
        factory.setS3Helper(mockS3Helper);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder()
                .withRecordIdS3Override("dummy-override-file").withUseLastExportTime(false).build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request, ImmutableMap.of());

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "s3-foo");
        assertEquals(recordIdList.get(1), "s3-bar");
        assertEquals(recordIdList.get(2), "s3-baz");
    }

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET))
                .thenReturn("dummy-override-bucket");
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME)).thenReturn("America/Los_Angeles");
        return mockConfig;
    }
}
