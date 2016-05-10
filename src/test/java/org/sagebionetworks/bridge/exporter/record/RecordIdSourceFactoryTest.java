package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyConditions;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class RecordIdSourceFactoryTest {
    @Test
    public void fromDdb() throws Exception {
        // mock DDB - The actual table index is just a dummy, since the query helper does all the real work.
        List<Item> ddbItemList = ImmutableList.of(new Item().withString("id", "ddb-foo"),
                new Item().withString("id", "ddb-bar"), new Item().withString("id", "ddb-baz"));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);
        when(mockQueryHelper.query(mockRecordIndex, "uploadDate", "2015-11-11")).thenReturn(ddbItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse("2015-11-11"))
                .build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "ddb-foo");
        assertEquals(recordIdList.get(1), "ddb-bar");
        assertEquals(recordIdList.get(2), "ddb-baz");
    }

    @Test
    public void fromDdbWithWhitelistAndDate() throws Exception {
        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(
                "2016-05-09"));
        fromDdbWithWhitelist(requestBuilder, DateTime.parse("2016-05-09T00:00:00.000-0700").getMillis(),
                DateTime.parse("2016-05-09T23:59:59.999-0700").getMillis());
    }

    @Test
    public void fromDdbWithWhitelistAndStartAndEndDateTime() throws Exception {
        DateTime startDateTime = DateTime.parse("2016-05-09T02:36:19.643-0700");
        DateTime endDateTime = DateTime.parse("2016-05-09T17:46:32.901-0700");

        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder()
                .withStartDateTime(startDateTime).withEndDateTime(endDateTime);

        fromDdbWithWhitelist(requestBuilder, startDateTime.getMillis(), endDateTime.getMillis() - 1);
    }

    private static void fromDdbWithWhitelist(BridgeExporterRequest.Builder requestBuilder, long expectedStartMillis,
            long expectedEndMillis) throws Exception {
        // mock DDB
        List<Item> barStudyItemList = ImmutableList.of(new Item().withString("id", "bar-1"),
                new Item().withString("id", "bar-2"));
        List<Item> fooStudyItemList = ImmutableList.of(new Item().withString("id", "foo-1"),
                new Item().withString("id", "foo-2"));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);

        ArgumentCaptor<RangeKeyCondition> barRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq("studyId"), eq("bar-study"), barRangeKeyCaptor.capture()))
                .thenReturn(barStudyItemList);

        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq("studyId"), eq("foo-study"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setConfig(mockConfig());
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);

        // finish building request - Use a TreeSet for the study whitelist, so we can get a deterministic test.
        // (ImmutableSet preserves the iteration order.)
        Set<String> studyWhitelist = new TreeSet<>();
        studyWhitelist.add("bar-study");
        studyWhitelist.add("foo-study");
        BridgeExporterRequest request = requestBuilder.withStudyWhitelist(studyWhitelist).build();

        // execute and validate
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 4);
        assertEquals(recordIdList.get(0), "bar-1");
        assertEquals(recordIdList.get(1), "bar-2");
        assertEquals(recordIdList.get(2), "foo-1");
        assertEquals(recordIdList.get(3), "foo-2");

        // validate range key queries
        validateRangeKey(barRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
        validateRangeKey(fooRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
    }

    private static void validateRangeKey(RangeKeyCondition rangeKey, long expectedStartMillis,
            long expectedEndMillis) {
        assertEquals(rangeKey.getAttrName(), "uploadedOn");
        assertEquals(rangeKey.getKeyCondition(), KeyConditions.BETWEEN);

        Object[] rangeKeyValueArray = rangeKey.getValues();
        assertEquals(rangeKeyValueArray.length, 2);
        assertEquals(rangeKeyValueArray[0], expectedStartMillis);
        assertEquals(rangeKeyValueArray[1], expectedEndMillis);
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
                .withRecordIdS3Override("dummy-override-file").build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "s3-foo");
        assertEquals(recordIdList.get(1), "s3-bar");
        assertEquals(recordIdList.get(2), "s3-baz");
    }

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(RecordIdSourceFactory.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET))
                .thenReturn("dummy-override-bucket");
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME)).thenReturn("America/Los_Angeles");
        return mockConfig;
    }
}
