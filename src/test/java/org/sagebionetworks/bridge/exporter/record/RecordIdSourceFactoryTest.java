package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
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
    public void fromS3Override() throws Exception {
        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(RecordIdSourceFactory.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET))
                .thenReturn("dummy-override-bucket");

        // mock S3
        List<String> s3Lines = ImmutableList.of("s3-foo", "s3-bar", "s3-baz");
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsLines("dummy-override-bucket", "dummy-override-file")).thenReturn(s3Lines);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setConfig(mockConfig);
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
}
