package org.sagebionetworks.bridge.exporter.dynamo;

// TODO doc
public class StudyInfo {
    private final long dataAccessTeamId;
    private final String synapseProjectId;

    private StudyInfo(long dataAccessTeamId, String synapseProjectId) {
        this.dataAccessTeamId = dataAccessTeamId;
        this.synapseProjectId = synapseProjectId;
    }

    public long getDataAccessTeamId() {
        return dataAccessTeamId;
    }

    public String getSynapseProjectId() {
        return synapseProjectId;
    }

    public static class Builder {
        private long dataAccessTeamId;
        private String synapseProjectId;

        public Builder withDataAccessTeamId(int dataAccessTeamId) {
            this.dataAccessTeamId = dataAccessTeamId;
            return this;
        }

        public Builder withSynapseProjectId(String synapseProjectId) {
            this.synapseProjectId = synapseProjectId;
            return this;
        }

        public StudyInfo build() {
            // TODO validate
            return new StudyInfo(dataAccessTeamId, synapseProjectId);
        }
    }
}
