package org.sagebionetworks.bridge.exporter.dynamo;

import org.apache.commons.lang3.StringUtils;

/**
 * This class encapsulates study config for Bridge-EX. Note that once the Synapse tables are created and the project is
 * populated, THIS SHOULD NOT BE CHANGED. Otherwise, bad things happen.
 */
public class StudyInfo {
    private final long dataAccessTeamId;
    private final String synapseProjectId;
    private final boolean disableExport;
    private final boolean studyIdExcludedInExport;
    private final boolean usesCustomExportSchedule;

    /** Private constructor. To construct, see builder. */
    private StudyInfo(long dataAccessTeamId, String synapseProjectId, boolean disableExport,
            boolean studyIdExcludedInExport, boolean usesCustomExportSchedule) {
        this.dataAccessTeamId = dataAccessTeamId;
        this.synapseProjectId = synapseProjectId;
        this.disableExport = disableExport;
        this.studyIdExcludedInExport = studyIdExcludedInExport;
        this.usesCustomExportSchedule = usesCustomExportSchedule;
    }

    /** The team ID of the team that is granted read access to exported data. */
    public long getDataAccessTeamId() {
        return dataAccessTeamId;
    }

    /** The Synapse project to export the data to. */
    public String getSynapseProjectId() {
        return synapseProjectId;
    }

    /** Flag indicating if this study should be excluded from exporting. */
    public boolean getDisableExport() {
        return disableExport;
    }

    /**
     * <p>
     * True if the Bridge Exporter should include the studyId prefix in the "originalTable" field in the appVersion
     * (now "Health Data Summary") table in Synapse. This exists primarily because we want to remove redundant prefixes
     * from the Synapse tables (to improve reporting), but we don't want to break existing studies or partition
     * existing data.
     * </p>
     * <p>
     * The setting is "reversed" so we don't have to backfill a bunch of old studies.
     * </p>
     * <p>
     * This is a "hidden" setting, primarily to support back-compat for old studies. New studies should be created with
     * this flag set to true, and only admins can change the flag.
     * </p>
     */
    public boolean isStudyIdExcludedInExport() {
        return studyIdExcludedInExport;
    }

    /**
     * False if BridgeEX should include it in the "default" nightly job (which is an export job without a study
     * whitelist). True otherwise.
     */
    public boolean getUsesCustomExportSchedule() {
        return usesCustomExportSchedule;
    }

    /** StudyInfo Builder. */
    public static class Builder {
        private Long dataAccessTeamId;
        private String synapseProjectId;
        private boolean disableExport;
        private boolean studyIdExcludedInExport;
        private boolean usesCustomExportSchedule;

        /** @see StudyInfo#getDataAccessTeamId */
        public Builder withDataAccessTeamId(long dataAccessTeamId) {
            this.dataAccessTeamId = dataAccessTeamId;
            return this;
        }

        /** @see StudyInfo#getSynapseProjectId */
        public Builder withSynapseProjectId(String synapseProjectId) {
            this.synapseProjectId = synapseProjectId;
            return this;
        }

        /** @see StudyInfo#getUsesCustomExportSchedule */
        public Builder withDisableExport(boolean disableExport) {
            this.disableExport = disableExport;
            return this;
        }

        /** @see StudyInfo#isStudyIdExcludedInExport */
        public Builder withStudyIdExcludedInExport(boolean studyIdExcludedInExport) {
            this.studyIdExcludedInExport = studyIdExcludedInExport;
            return this;
        }

        /** @see StudyInfo#getUsesCustomExportSchedule */
        public Builder withUsesCustomExportSchedule(boolean usesCustomExportSchedule) {
            this.usesCustomExportSchedule = usesCustomExportSchedule;
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
            }

            return new StudyInfo(dataAccessTeamId, synapseProjectId, disableExport, studyIdExcludedInExport,
                    usesCustomExportSchedule);
        }
    }
}
