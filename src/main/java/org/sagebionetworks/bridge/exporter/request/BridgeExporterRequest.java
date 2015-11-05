package org.sagebionetworks.bridge.exporter.request;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.json.LocalDateToStringSerializer;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO doc
@JsonDeserialize(builder = BridgeExporterRequest.Builder.class)
public class BridgeExporterRequest {
    private final LocalDate date;
    private final String exporterDdbPrefixOverride;
    private final String recordIdS3Override;
    private final BridgeExporterSharingMode sharingMode;
    private final Set<String> studyWhitelist;
    private final Map<String, String> synapseProjectOverrideMap;
    private final Set<UploadSchemaKey> tableWhitelist;
    private final String tag;

    // TODO doc
    private BridgeExporterRequest(LocalDate date, String exporterDdbPrefixOverride, String recordIdS3Override,
            BridgeExporterSharingMode sharingMode, Set<String> studyWhitelist,
            Map<String, String> synapseProjectOverrideMap, Set<UploadSchemaKey> tableWhitelist, String tag) {
        this.date = date;
        this.exporterDdbPrefixOverride = exporterDdbPrefixOverride;
        this.recordIdS3Override = recordIdS3Override;
        this.sharingMode = sharingMode;
        this.studyWhitelist = studyWhitelist;
        this.synapseProjectOverrideMap = synapseProjectOverrideMap;
        this.tableWhitelist = tableWhitelist;
        this.tag = tag;
    }

    @JsonSerialize(using = LocalDateToStringSerializer.class)
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

    public Set<String> getStudyWhitelist() {
        return studyWhitelist;
    }

    public Map<String, String> getSynapseProjectOverrideMap() {
        return synapseProjectOverrideMap;
    }

    public Set<UploadSchemaKey> getTableWhitelist() {
        return tableWhitelist;
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
        private Set<String> studyWhitelist;
        private Map<String, String> synapseProjectOverride;
        private Set<UploadSchemaKey> tableWhitelist;
        private String tag;

        @JsonDeserialize(using = LocalDateDeserializer.class)
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

        public Builder withStudyWhitelist(Set<String> studyWhitelist) {
            this.studyWhitelist = studyWhitelist;
            return this;
        }

        public Builder withSynapseProjectOverride(Map<String, String> synapseProjectOverride) {
            this.synapseProjectOverride = synapseProjectOverride;
            return this;
        }

        public Builder withTableWhitelist(Set<UploadSchemaKey> tableWhitelist) {
            this.tableWhitelist = tableWhitelist;
            return this;
        }

        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public BridgeExporterRequest build() {
            // TODO validate, defaults
            // TODO validate study and table whitelists and project override map can be null, but cannot be empty
            if (sharingMode == null) {
                sharingMode = BridgeExporterSharingMode.SHARED;
            }
            return new BridgeExporterRequest(date, exporterDdbPrefixOverride, recordIdS3Override, sharingMode,
                    studyWhitelist, synapseProjectOverride, tableWhitelist, tag);
        }
    }
}
