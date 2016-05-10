package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

/**
 * Factory class to construct the appropriate RecordIdSource for the given request. This class abstracts away logic for
 * initializing a RecordIdSource from a DynamoDB query or from a record override file in S3.
 */
@Component
public class RecordIdSourceFactory {
    // package-scoped to be visible to unit tests
    static final String CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET = "record.id.override.bucket";

    private static final RecordIdSource.Converter<Item> DYNAMO_ITEM_CONVERTER = from -> from.getString("id");
    private static final RecordIdSource.Converter<String> NOOP_CONVERTER = from -> from;

    // config vars
    private String overrideBucket;
    private DateTimeZone timeZone;

    // Spring helpers
    private DynamoQueryHelper ddbQueryHelper;
    private Index ddbRecordStudyUploadedOnIndex;
    private Index ddbRecordUploadDateIndex;
    private S3Helper s3Helper;

    /** Config, used to get S3 bucket for record ID override files. */
    @Autowired
    final void setConfig(Config config) {
        overrideBucket = config.get(CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
        timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** DDB Query Helper, used to abstract away query logic. */
    @Autowired
    final void setDdbQueryHelper(DynamoQueryHelper ddbQueryHelper) {
        this.ddbQueryHelper = ddbQueryHelper;
    }

    /** DDB Record table studyId-uploadedOn index. */
    @Resource(name = "ddbRecordStudyUploadedOnIndex")
    final void setDdbRecordStudyUploadedOnIndex(Index ddbRecordStudyUploadedOnIndex) {
        this.ddbRecordStudyUploadedOnIndex = ddbRecordStudyUploadedOnIndex;
    }

    /** DDB Record table Upload Date index. */
    @Resource(name = "ddbRecordUploadDateIndex")
    final void setDdbRecordUploadDateIndex(Index ddbRecordUploadDateIndex) {
        this.ddbRecordUploadDateIndex = ddbRecordUploadDateIndex;
    }

    /** S3 Helper, used to download record ID override files. */
    @Autowired
    final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /**
     * Gets the record ID source for the given Bridge EX request. Returns an Iterable instead of a RecordIdSource for
     * easy mocking.
     *
     * @param request
     *         Bridge EX request
     * @return record ID source
     * @throws IOException
     *         if we fail reading the underlying source
     */
    public Iterable<String> getRecordSourceForRequest(BridgeExporterRequest request) throws IOException {
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return getS3RecordIdSource(request);
        } else if (request.getStudyWhitelist() != null) {
            return getDynamoRecordIdSourceWithStudyWhitelist(request);
        } else {
            return getDynamoRecordIdSource(request);
        }
    }

    /**
     * If we have a study whitelist, we should always use the study-uploadedOn index, as it's more performant. If we
     * have a date, we'll need to compute the start and endDateTimes based off of that. Otherwise, we use the start-
     * and endDateTimes verbatim.
     */
    private Iterable<String> getDynamoRecordIdSourceWithStudyWhitelist(BridgeExporterRequest request) {
        // Compute start- and endDateTime.
        DateTime startDateTime;
        DateTime endDateTime;
        if (request.getDate() != null) {
            // startDateTime is obviously date at midnight local time. endDateTime is the start of the next day.
            LocalDate date = request.getDate();
            startDateTime = date.toDateTimeAtStartOfDay(timeZone);
            endDateTime = date.plusDays(1).toDateTimeAtStartOfDay(timeZone);
        } else {
            // Logically, if there is no date, there must be a start- and endDateTime. (We check for recordIdS3Override
            // in the calling method.
            startDateTime = request.getStartDateTime();
            endDateTime = request.getEndDateTime();
        }

        // startDateTime is inclusive but endDateTime is exclusive. This is so that if a record happens to fall right
        // on the overlap, it's only exported once. DDB doesn't allow us to do AND conditions in a range key query,
        // and the BETWEEN is always inclusive. However, the granularity is down to the millisecond, so we can just
        // use endDateTime minus 1 millisecond.
        long startEpochTime = startDateTime.getMillis();
        long endEpochTime = endDateTime.getMillis() - 1;

        // RangeKeyCondition can be shared across multiple queries.
        RangeKeyCondition rangeKeyCondition = new RangeKeyCondition("uploadedOn").between(startEpochTime,
                endEpochTime);

        // We need to make a separate query for _each_ study in the whitelist. That's just how DDB hash keys work.
        List<Iterable<Item>> recordItemIterList = new ArrayList<>();
        for (String oneStudyId : request.getStudyWhitelist()) {
            Iterable<Item> recordItemIter = ddbQueryHelper.query(ddbRecordStudyUploadedOnIndex, "studyId", oneStudyId,
                    rangeKeyCondition);
            recordItemIterList.add(recordItemIter);
        }

        // Concatenate all the iterables together.
        return new RecordIdSource<>(Iterables.concat(recordItemIterList), DYNAMO_ITEM_CONVERTER);
    }

    /** Get the record ID source from a DDB query. */
    private Iterable<String> getDynamoRecordIdSource(BridgeExporterRequest request) {
        Iterable<Item> recordItemIter = ddbQueryHelper.query(ddbRecordUploadDateIndex, "uploadDate",
                request.getDate().toString());
        return new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER);
    }

    /**
     * Get the record ID source from a record override file in S3. We assume the list of record IDs is small enough to
     * reasonably fit in memory.
     */
    private Iterable<String> getS3RecordIdSource(BridgeExporterRequest request) throws IOException {
        List<String> recordIdList = s3Helper.readS3FileAsLines(overrideBucket, request.getRecordIdS3Override());
        return new RecordIdSource<>(recordIdList, NOOP_CONVERTER);
    }
}
