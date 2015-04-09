package org.sagebionetworks.bridge.exporter;

import java.util.List;
import java.util.Map;

/**
 * Bridge Exporter config. This is currently loaded from bridge-synapse-exporter-config.json in the user's home
 * directory. This is used to configure production and development environments independently.
 */
public class BridgeExporterConfig {
    private String apiKey;
    private Map<String, Long> dataAccessTeamIdsByStudy;
    private String ddbPrefix;
    private String username;
    private int numThreads;
    private String password;
    private long principalId;
    private Map<String, String> projectIdsByStudy;
    private List<String> studyIdList;

    /** Synapse API Key. */
    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Data access team for each study. This team will be given read and download priveleges for each Synapse table.
     */
    public Map<String, Long> getDataAccessTeamIdsByStudy() {
        return dataAccessTeamIdsByStudy;
    }

    public void setDataAccessTeamIdsByStudy(Map<String, Long> dataAccessTeamIdsByStudy) {
        this.dataAccessTeamIdsByStudy = dataAccessTeamIdsByStudy;
    }

    /**
     * Prefix for the DDB tables holding the Synapse table names and table IDs.
     */
    public String getDdbPrefix() {
        return ddbPrefix;
    }

    public void setDdbPrefix(String ddbPrefix) {
        this.ddbPrefix = ddbPrefix;
    }

    /** Synapse user name of the Bridge Exporter Synapse account. */
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /** Number of threads to run in the exporter. */
    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    /**
     * Synapse password of the Bridge Exporter Synapse account. Due to bug
     * https://sagebionetworks.jira.com/browse/PLFM-3310, this is needed to create file handles.
     */
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /** Synapse principal ID of the Bridge Exporter Synapse account. */
    public long getPrincipalId() {
        return principalId;
    }

    public void setPrincipalId(long principalId) {
        this.principalId = principalId;
    }

    /** Synapse project IDs corresponding to each study. */
    public Map<String, String> getProjectIdsByStudy() {
        return projectIdsByStudy;
    }

    public void setProjectIdsByStudy(Map<String, String> projectIdsByStudy) {
        this.projectIdsByStudy = projectIdsByStudy;
    }

    /** List of study IDs to export. */
    public List<String> getStudyIdList() {
        return studyIdList;
    }

    public void setStudyIdList(List<String> studyIdList) {
        this.studyIdList = studyIdList;
    }
}
