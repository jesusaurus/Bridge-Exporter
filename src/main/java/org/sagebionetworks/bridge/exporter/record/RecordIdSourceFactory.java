package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.commons.lang.StringUtils;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.s3.S3Helper;

// TODO doc
public class RecordIdSourceFactory {
    static final String CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET = "record.id.override.bucket";

    private static final RecordIdSource.Converter<Item> DYNAMO_ITEM_CONVERTER = from -> from.getString("id");
    private static final RecordIdSource.Converter<String> NOOP_CONVERTER = from -> from;

    private Index ddbRecordUploadDateIndex;
    private DynamoQueryHelper ddbQueryHelper;
    private String overrideBucket;
    private S3Helper s3Helper;

    public final void setConfig(Config config) {
        overrideBucket = config.get(CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
    }

    public final void setDdbRecordUploadDateIndex(Index ddbRecordUploadDateIndex) {
        this.ddbRecordUploadDateIndex = ddbRecordUploadDateIndex;
    }

    public final void setDdbQueryHelper(DynamoQueryHelper ddbQueryHelper) {
        this.ddbQueryHelper = ddbQueryHelper;
    }

    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public Iterable<String> getRecordSourceForRequest(BridgeExporterRequest request) throws IOException {
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return getS3RecordIdSource(request);
        } else {
            return getDynamoRecordIdSource(request);
        }
    }

    private Iterable<String> getDynamoRecordIdSource(BridgeExporterRequest request) {
        Iterable<Item> recordItemIter = ddbQueryHelper.query(ddbRecordUploadDateIndex, "uploadDate",
                request.getDate().toString());
        return new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER);
    }

    private Iterable<String> getS3RecordIdSource(BridgeExporterRequest request) throws IOException {
        List<String> recordIdList = s3Helper.readS3FileAsLines(overrideBucket, request.getRecordIdS3Override());
        return new RecordIdSource<>(recordIdList, NOOP_CONVERTER);
    }
}
