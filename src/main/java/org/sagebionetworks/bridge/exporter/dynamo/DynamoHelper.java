package org.sagebionetworks.bridge.exporter.dynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.jcabi.aspects.Cacheable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/**
 * Encapsulates common calls Bridge-EX makes to DDB. Some of these have a little bit of logic or require marshalling
 * from a DDB item to a Bridge-EX object. Others are there just for convenience, so we have all or most of our DDB
 * interactions through a single class.
 */
@Component
public class DynamoHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoHelper.class);

    private static final String STUDY_INFO_KEY_DATA_ACCESS_TEAM = "synapseDataAccessTeamId";
    private static final String STUDY_INFO_KEY_PROJECT_ID = "synapseProjectId";
    private static final String STUDY_DISABLE_EXPORT = "disableExport";
    private static final String STUDY_INFO_KEY_STUDY_ID_EXCLUDED_IN_EXPORT = "studyIdExcludedInExport";
    private static final String STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE = "usesCustomExportSchedule";

    static final String IDENTIFIER = "identifier";
    static final String LAST_EXPORT_DATE_TIME = "lastExportDateTime";
    static final String STUDY_ID = "studyId";

    private Table ddbStudyTable;
    private Table ddbExportTimeTable;
    private DynamoScanHelper ddbScanHelper;
    private DateTimeZone timeZone;

    // Rate limiter, used to limit the amount of traffic to DDB, specifically for when we loop over a potentially
    // unbounded series of studies. Conservatively limit at 1 req/sec.
    private final RateLimiter rateLimiter = RateLimiter.create(1.0);

    /** Config, used to get S3 bucket for record ID override files. */
    @Autowired
    final void setConfig(Config config) {
        timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** Study table, used to get study config, like linked Synapse project. */
    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    /** DDB Export Time Table. */
    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
    }

    @Autowired
    final void setDdbScanHelper(DynamoScanHelper ddbScanHelper) {
        this.ddbScanHelper = ddbScanHelper;
    }

    /**
     * Get study info, namely Synapse project and data access team.
     *
     * @param studyId
     *         study ID to fetch
     * @return study info
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public StudyInfo getStudyInfo(String studyId) {
        Item studyItem = ddbStudyTable.getItem("identifier", studyId);
        if (studyItem == null) {
            return null;
        }

        // DDB's Item.getLong() will throw if the value is null. For robustness, check get() the value and check that
        // it's not null. (This should cover both cases where the attribute doesn't exist and cases where the attribute
        // exists and is null.
        StudyInfo.Builder studyInfoBuilder = new StudyInfo.Builder();

        // we need to check at first if the study is disabled exporting or not
        // treat null as false

        if (studyItem.get(STUDY_INFO_KEY_DATA_ACCESS_TEAM) != null) {
            studyInfoBuilder.withDataAccessTeamId(studyItem.getLong(STUDY_INFO_KEY_DATA_ACCESS_TEAM));
        }

        if (studyItem.get(STUDY_INFO_KEY_PROJECT_ID) != null) {
            studyInfoBuilder.withSynapseProjectId(studyItem.getString(STUDY_INFO_KEY_PROJECT_ID));
        }

        if (studyItem.get(STUDY_DISABLE_EXPORT) != null) {
            // For some reason, the mapper saves the value as an int, not as a boolean.
            boolean disableExport = parseDdbBoolean(studyItem, STUDY_DISABLE_EXPORT);
            studyInfoBuilder.withDisableExport(disableExport);
        }

        if (studyItem.get(STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE) != null) {
            boolean usesCustomExportSchedule = parseDdbBoolean(studyItem, STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE);
            studyInfoBuilder.withUsesCustomExportSchedule(usesCustomExportSchedule);
        }

        if (studyItem.get(STUDY_INFO_KEY_STUDY_ID_EXCLUDED_IN_EXPORT) != null) {
            boolean studyIdExcludedInExport = parseDdbBoolean(studyItem, STUDY_INFO_KEY_STUDY_ID_EXCLUDED_IN_EXPORT);
            studyInfoBuilder.withStudyIdExcludedInExport(studyIdExcludedInExport);
        }

        return studyInfoBuilder.build();
    }

    /**
     * Helper method to generate study ids for query.
     *
     * @return A Map with key is study id, value is the start date time to query ddb table.
     */
    public Map<String, DateTime> bootstrapStudyIdsToQuery(BridgeExporterRequest request)
            throws PollSqsWorkerBadRequestException {
        DateTime endDateTime = request.getEndDateTime();
        if (endDateTime == null) {
            // This request doesn't use start/endDate time. The logic here can be skipped.
            return ImmutableMap.of();
        }

        // Is this a custom job? A custom job is defined as a job that specifies the study whitelist instead of
        // exporting all studies.
        Set<String> studyWhitelist = request.getStudyWhitelist();
        boolean isCustomJob = (studyWhitelist != null);

        // Figure out which studies we need to process.
        List<String> studyIdList = new ArrayList<>();
        if (studyWhitelist == null) {
            // get the study id list from ddb table
            Iterable<Item> scanOutcomes = ddbScanHelper.scan(ddbStudyTable);
            for (Item item: scanOutcomes) {
                studyIdList.add(item.getString(IDENTIFIER));
            }
        } else {
            studyIdList.addAll(studyWhitelist);
        }

        // Filter out studies based on study configuration.
        Iterator<String> studyIdListIter = studyIdList.iterator();
        while (studyIdListIter.hasNext()) {
            rateLimiter.acquire();

            String studyId = studyIdListIter.next();

            // Get study info to determine whether to include this study in the export job.
            StudyInfo studyInfo = getStudyInfo(studyId);
            if (studyInfo == null || studyInfo.getDisableExport()) {
                // Unconfigured or disabled. Skip study.
                studyIdListIter.remove();
            } else if (studyInfo.getUsesCustomExportSchedule() && !isCustomJob) {
                // This study requires a study whitelist, but we don't have one. (If we *do* have a study
                // whitelist, because of the logic above, this study must be in that whitelist.)
                studyIdListIter.remove();
            }
        }

        // Figure out the time range (start time) for each study.
        Map<String, DateTime> studyIdsToQuery = new HashMap<>();
        if (request.getStartDateTime() != null) {
            // If we specified the start time, it's easy.
            for (String oneStudyId : studyIdList) {
                studyIdsToQuery.put(oneStudyId, request.getStartDateTime());
            }
        } else if (request.getUseLastExportTime()) {
            for (String studyId : studyIdList) {
                rateLimiter.acquire();

                // If we're using last export time, query that for each study.
                DateTime lastExportDateTime;
                Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
                if (studyIdItem != null) {
                    lastExportDateTime = new DateTime(studyIdItem.getLong(LAST_EXPORT_DATE_TIME), timeZone);
                } else {
                    // If there's no last export time, bootstrap by exporting everything since the beginning of
                    // yesterday.
                    lastExportDateTime = endDateTime.minusDays(1).withTimeAtStartOfDay();
                }

                // If the time range is zero, don't export.
                if (lastExportDateTime.isBefore(endDateTime)) {
                    studyIdsToQuery.put(studyId, lastExportDateTime);
                }
            }
        } else {
            throw new PollSqsWorkerBadRequestException("Request has neither startDateTime nor useLastExportTime=true");
        }

        return studyIdsToQuery;
    }

    /**
     * Helper method to update ddb exportTimeTable
     */
    public void updateExportTimeTable(List<String> studyIdsToUpdate, DateTime endDateTime) {
        if (!studyIdsToUpdate.isEmpty() && endDateTime != null) {
            for (String studyId: studyIdsToUpdate) {
                rateLimiter.acquire();

                try {
                    ddbExportTimeTable.putItem(new Item().withPrimaryKey(STUDY_ID, studyId).withNumber(LAST_EXPORT_DATE_TIME, endDateTime.getMillis()));
                } catch (RuntimeException ex) {
                    LOG.error("Unable to update export time table for study id: " + studyId +
                            ex.getMessage(), ex);
                }
            }
        }
    }

    /**
     * Helper function to parse ddb boolean value into boolean
     */
    private boolean parseDdbBoolean(Item item, String attributeName) {
        return item.getInt(attributeName) != 0;
    }
}
