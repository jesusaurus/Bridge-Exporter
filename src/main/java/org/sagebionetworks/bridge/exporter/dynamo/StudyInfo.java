package org.sagebionetworks.bridge.exporter.dynamo;

import org.apache.commons.lang.StringUtils;

/**
 * This class encapsulates study config for Bridge-EX. Note that once the Synapse tables are created and the project is
 * populated, THIS SHOULD NOT BE CHANGED. Otherwise, bad things happen.
 */
public class StudyInfo {
    private final Long dataAccessTeamId;
    private final String synapseProjectId;

    /** Private constructor. To construct, see builder. */
    private StudyInfo(Long dataAccessTeamId, String synapseProjectId) {
        this.dataAccessTeamId = dataAccessTeamId;
        this.synapseProjectId = synapseProjectId;
    }

    /** The team ID of the team that is granted read access to exported data. */
    public Long getDataAccessTeamId() {
        return dataAccessTeamId;
    }

    /** The Synapse project to export the data to. */
    public String getSynapseProjectId() {
        return synapseProjectId;
    }

    /** StudyInfo Builder. */
    public static class Builder {
        private Long dataAccessTeamId;
        private String synapseProjectId;

        /** @see StudyInfo#getDataAccessTeamId */
        public Builder withDataAccessTeamId(Long dataAccessTeamId) {
            this.dataAccessTeamId = dataAccessTeamId;
            return this;
        }

        /** @see StudyInfo#getSynapseProjectId */
        public Builder withSynapseProjectId(String synapseProjectId) {
            this.synapseProjectId = synapseProjectId;
            return this;
        }

        /**
         * Builds the study info object. The study may not have been configured for Bridge-EX yet (that is, no
         * dataAccessTeamId and no synapseProjectId). If that's the case, return null instead of returning an
         * incomplete StudyInfo.
         */
        public StudyInfo build() {
            if (dataAccessTeamId == null || StringUtils.isBlank(synapseProjectId)) {
                return null;
            } else {
                return new StudyInfo(dataAccessTeamId, synapseProjectId);
            }
        }
    }
}
