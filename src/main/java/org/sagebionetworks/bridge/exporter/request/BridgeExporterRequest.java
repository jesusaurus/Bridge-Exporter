package org.sagebionetworks.bridge.exporter.request;

import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.exporter.dynamo.BridgeExporterSharingMode;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO doc
// TODO Jackson annotations
public class BridgeExporterRequest {
    private final LocalDate date;
    private final String exporterDdbPrefixOverride;
    private final String recordIdS3Override;
    private final BridgeExporterSharingMode sharingMode;
    private final Set<String> studyFilterSet;
    private final Map<String, String> synapseProjectOverrideMap;
    private final Set<UploadSchemaKey> tableFilterSet;
    private final String tag;

    // TODO doc
    private BridgeExporterRequest(LocalDate date, String exporterDdbPrefixOverride, String recordIdS3Override,
            BridgeExporterSharingMode sharingMode, Set<String> studyFilterSet,
            Map<String, String> synapseProjectOverrideMap, Set<UploadSchemaKey> tableFilterSet, String tag) {
        this.date = date;
        this.exporterDdbPrefixOverride = exporterDdbPrefixOverride;
        this.recordIdS3Override = recordIdS3Override;
        this.sharingMode = sharingMode;
        this.studyFilterSet = studyFilterSet;
        this.synapseProjectOverrideMap = synapseProjectOverrideMap;
        this.tableFilterSet = tableFilterSet;
        this.tag = tag;
    }

    public LocalDate getDate() {
        return date;
    }

    public String getExporterDdbPrefixOverride() {
        return exporterDdbPrefixOverride;
    }

    public String getRecordIdS3Override() {
        return recordIdS3Override;
    }

    public BridgeExporterSharingMode getSharingMode() {
        return sharingMode;
    }

    public Set<String> getStudyFilterSet() {
        return studyFilterSet;
    }

    public Map<String, String> getSynapseProjectOverrideMap() {
        return synapseProjectOverrideMap;
    }

    public Set<UploadSchemaKey> getTableFilterSet() {
        return tableFilterSet;
    }

    public String getTag() {
        return tag;
    }

    // TODO doc
    public static class Builder {
        private LocalDate date;
        private String exporterDdbPrefixOverride;
        private String recordIdS3Override;
        private BridgeExporterSharingMode sharingMode;
        private Set<String> studyFilterSet;
        private Map<String, String> synapseProjectOverride;
        private Set<UploadSchemaKey> tableFilterSet;
        private String tag;

        public Builder withDate(LocalDate date) {
            this.date = date;
            return this;
        }

        public Builder withExporterDdbPrefixOverride(String exporterDdbPrefixOverride) {
            this.exporterDdbPrefixOverride = exporterDdbPrefixOverride;
            return this;
        }

        public Builder withRecordIdS3Override(String recordIdS3Override) {
            this.recordIdS3Override = recordIdS3Override;
            return this;
        }

        public Builder withSharingMode(BridgeExporterSharingMode sharingMode) {
            this.sharingMode = sharingMode;
            return this;
        }

        public Builder withStudyFilterSet(Set<String> studyFilterSet) {
            this.studyFilterSet = studyFilterSet;
            return this;
        }

        public Builder withSynapseProjectOverride(Map<String, String> synapseProjectOverride) {
            this.synapseProjectOverride = synapseProjectOverride;
            return this;
        }

        public Builder withTableFilterSet(Set<UploadSchemaKey> tableFilterSet) {
            this.tableFilterSet = tableFilterSet;
            return this;
        }

        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public BridgeExporterRequest build() {
            // TODO validate, defaults
            return new BridgeExporterRequest(date, exporterDdbPrefixOverride, recordIdS3Override, sharingMode,
                    studyFilterSet, synapseProjectOverride, tableFilterSet, tag);
        }
    }
}
