package org.sagebionetworks.bridge.exporter.study;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.cache.CacheLoader;

// TODO doc
public class StudyInfoCacheLoader extends CacheLoader<String, StudyInfo> {
    private Table ddbStudyTable;

    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    @Override
    public StudyInfo load(String studyId) {
        // TODO add these fields to the study table
        Item studyItem = ddbStudyTable.getItem("identifier", studyId);
        return new StudyInfo.Builder().withDataAccessTeamId(studyItem.getInt("synapseDataAccessTeamId"))
                .withSynapseProjectId("synapseProjectId").build();
    }
}
