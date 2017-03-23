package org.sagebionetworks.bridge.exporter.record;

import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterSharingMode;
import org.sagebionetworks.bridge.exporter.dynamo.SharingScope;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * Encapsulates logic for filtering out records from the export. This covers filtering because of request args (study
 * whitelist or table whitelist), or filtering because of sharing preferences.
 */
@Component
public class RecordFilterHelper {
    private static final Logger LOG = LoggerFactory.getLogger(RecordFilterHelper.class);

    private DynamoHelper dynamoHelper;

    /** Dynamo Helper, used to get a user's sharing preferences. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /**
     * Returns true if a record should be excluded from the export.
     *
     * @param metrics
     *         metrics object, to record filter metrics
     * @param request
     *         export request, used for determining filter settings
     * @param record
     *         record to determine if we should include or exclude
     * @return true if the record should be excluded
     */
    public boolean shouldExcludeRecord(Metrics metrics, BridgeExporterRequest request, Item record) {
        // If record doesn't have a study ID, something is seriously wrong.
        String studyId = record.getString("studyId");
        if (StringUtils.isBlank(studyId)) {
            throw new IllegalArgumentException("record has no study ID");
        }

        // request always has a sharing mode
        boolean excludeBySharingScope = shouldExcludeRecordBySharingScope(metrics, request.getSharingMode(), record);

        // filter by study - This is used for filtering out test studies and for limiting study-specific exports.
        boolean excludeByStudy = false;
        Set<String> studyWhitelist = request.getStudyWhitelist();
        if (studyWhitelist != null) {
            excludeByStudy = shouldExcludeRecordByStudy(metrics, studyWhitelist, studyId);
        }

        // filter by table - This is used for table-specific redrives.
        boolean excludeByTable = false;
        Set<UploadSchemaKey> tableWhitelist = request.getTableWhitelist();
        if (tableWhitelist != null) {
            excludeByTable = shouldExcludeRecordByTable(metrics, tableWhitelist,
                    BridgeExporterUtil.getSchemaKeyForRecord(record));
        }

        // filter based on study configuration
        boolean excludeByStudyConfig = shouldExcludeByStudyConfiguration(metrics, studyWhitelist, studyId);

        // If any of the filters are hit, we filter the record. (We don't use short-circuiting because we want to
        // collect the metrics.)
        return excludeBySharingScope || excludeByStudy || excludeByTable || excludeByStudyConfig;
    }

    // Helper method that handles the sharing filter.
    private boolean shouldExcludeRecordBySharingScope(Metrics metrics, BridgeExporterSharingMode sharingMode,
            Item record) {
        // Get the record's sharing scope. Defaults to no_sharing if it's not present or unable to be parsed.
        SharingScope recordSharingScope;
        String recordSharingScopeStr = record.getString("userSharingScope");
        if (StringUtils.isBlank(recordSharingScopeStr)) {
            recordSharingScope = SharingScope.NO_SHARING;
        } else {
            try {
                recordSharingScope = SharingScope.valueOf(recordSharingScopeStr);
            } catch (IllegalArgumentException ex) {
                LOG.error("Could not parse sharing scope " + recordSharingScopeStr);
                recordSharingScope = SharingScope.NO_SHARING;
            }
        }

        // Get sharing scope from user's participant options. Dynamo Helper takes care of defaulting to NO_SHARING.
        String healthCode = record.getString("healthCode");
        SharingScope userSharingScope = dynamoHelper.getSharingScopeForUser(healthCode);

        // reconcile both sharing scopes to find the most restrictive sharing scope
        SharingScope sharingScope;
        if (SharingScope.NO_SHARING.equals(recordSharingScope) || SharingScope.NO_SHARING.equals(userSharingScope)) {
            sharingScope = SharingScope.NO_SHARING;
        } else if (SharingScope.SPONSORS_AND_PARTNERS.equals(recordSharingScope) ||
                SharingScope.SPONSORS_AND_PARTNERS.equals(userSharingScope)) {
            sharingScope = SharingScope.SPONSORS_AND_PARTNERS;
        } else if (SharingScope.ALL_QUALIFIED_RESEARCHERS.equals(recordSharingScope) ||
                SharingScope.ALL_QUALIFIED_RESEARCHERS.equals(userSharingScope)) {
            sharingScope = SharingScope.ALL_QUALIFIED_RESEARCHERS;
        } else {
            throw new IllegalArgumentException("Impossible code path in RecordFilterHelper.shouldExcludeRecordBySharingScope(): recordSharingScope=" +
                    recordSharingScope + ", userSharingScope=" + userSharingScope);
        }

        // actual filter logic here
        if (sharingMode.shouldExcludeScope(sharingScope)) {
            metrics.incrementCounter("excluded[" + sharingScope.name() + "]");
            return true;
        } else {
            metrics.incrementCounter("accepted[" + sharingScope.name() + "]");
            return false;
        }
    }

    // Helper method that handles the study filter.
    private boolean shouldExcludeRecordByStudy(Metrics metrics, Set<String> studyFilterSet, String studyId) {
        // studyFilterSet is the set of studies that we accept
        if (studyFilterSet.contains(studyId)) {
            metrics.incrementCounter("accepted[" + studyId + "]");
            return false;
        } else {
            metrics.incrementCounter("excluded[" + studyId + "]");
            return true;
        }
    }

    // Helper method that handles the table filter.
    private boolean shouldExcludeRecordByTable(Metrics metrics, Set<UploadSchemaKey> tableFilterSet,
            UploadSchemaKey schemaKey) {
        // tableFilterSet is the set of tables that we accept
        if (tableFilterSet.contains(schemaKey)) {
            metrics.incrementCounter("accepted[" + schemaKey + "]");
            return false;
        } else {
            metrics.incrementCounter("excluded[" + schemaKey + "]");
            return true;
        }
    }

    // Helper method that fetches and filters based on study configuration
    private boolean shouldExcludeByStudyConfiguration(Metrics metrics, Set<String> studyWhitelist, String studyId) {
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);

        // Configured studies will return non-null studyInfo. Unconfigured studies will return null ones.
        if (studyInfo == null) {
            metrics.incrementCounter("unconfigured[" + studyId + "]");
            return true;
        }

        if (studyInfo.getDisableExport()) {
            metrics.incrementCounter("disabled-export study[" + studyId + "]");
            return true;
        }

        // If the study doesn't have a custom export schedule, it's included.
        if (!studyInfo.getUsesCustomExportSchedule()) {
            metrics.incrementCounter("configured[" + studyId + "]");
            return false;
        }

        if (studyWhitelist != null && studyWhitelist.contains(studyId)) {
            // If there is a study whitelist that includes this study, then this is a custom export job. Include the
            // record.
            metrics.incrementCounter("custom-export-accepted[" + studyId + "]");
            return false;
        } else {
            // This is not a custom job. We shouldn't include the record.
            metrics.incrementCounter("custom-export-excluded[" + studyId + "]");
            return true;
        }
    }
}
