package org.sagebionetworks.bridge.exporter.study;

// TODO doc
public class StudyInfo {
    private final int dataAccessTeamId;
    private final String synapseProjectId;

    private StudyInfo(int dataAccessTeamId, String synapseProjectId) {
        this.dataAccessTeamId = dataAccessTeamId;
        this.synapseProjectId = synapseProjectId;
    }

    public int getDataAccessTeamId() {
        return dataAccessTeamId;
    }

    public String getSynapseProjectId() {
        return synapseProjectId;
    }

    public static class Builder {
        private int dataAccessTeamId;
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
