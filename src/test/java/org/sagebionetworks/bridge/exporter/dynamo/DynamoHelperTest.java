package org.sagebionetworks.bridge.exporter.dynamo;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper.STUDY_DISABLE_EXPORT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class DynamoHelperTest {
    @Test
    public void getSharingScope() {
        // mock DDB Participant Options table
        String optsDataJson = "{\"SHARING_SCOPE\":\"SPONSORS_AND_PARTNERS\"}";
        Item partOptsItem = new Item().withString("data", optsDataJson);

        Table mockPartOptsTable = mock(Table.class);
        when(mockPartOptsTable.getItem("healthDataCode", "normal-health-code")).thenReturn(partOptsItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbParticipantOptionsTable(mockPartOptsTable);

        // execute and validate
        SharingScope sharingScope = helper.getSharingScopeForUser("normal-health-code");
        assertEquals(sharingScope, SharingScope.SPONSORS_AND_PARTNERS);
    }

    @Test
    public void ddbErrorGettingSharingScope() {
        // mock DDB Participant Options table
        Table mockPartOptsTable = mock(Table.class);
        when(mockPartOptsTable.getItem("healthDataCode", "ddb-error-health-code"))
                .thenThrow(AmazonClientException.class);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbParticipantOptionsTable(mockPartOptsTable);

        // execute and validate - defaults to no_sharing
        SharingScope sharingScope = helper.getSharingScopeForUser("ddb-error-health-code");
        assertEquals(sharingScope, SharingScope.NO_SHARING);
    }

    @Test
    public void noParticipantOptions() {
        // mock DDB Participant Options table
        Table mockPartOptsTable = mock(Table.class);
        when(mockPartOptsTable.getItem("healthDataCode", "missing-health-code")).thenReturn(null);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbParticipantOptionsTable(mockPartOptsTable);

        // execute and validate - defaults to no_sharing
        SharingScope sharingScope = helper.getSharingScopeForUser("missing-health-code");
        assertEquals(sharingScope, SharingScope.NO_SHARING);
    }

    @Test
    public void noSharingScopeInParticipantOptions() {
        // mock DDB Participant Options table
        Item partOptsItem = new Item().withString("data", "{}");

        Table mockPartOptsTable = mock(Table.class);
        when(mockPartOptsTable.getItem("healthDataCode", "missing-sharing-health-code")).thenReturn(partOptsItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbParticipantOptionsTable(mockPartOptsTable);

        // execute and validate - defaults to no_sharing
        SharingScope sharingScope = helper.getSharingScopeForUser("missing-sharing-health-code");
        assertEquals(sharingScope, SharingScope.NO_SHARING);
    }

    @Test
    public void errorParsingSharingScope() {
        // mock DDB Participant Options table
        String optsDataJson = "{\"SHARING_SCOPE\":\"foobarbaz\"}";
        Item partOptsItem = new Item().withString("data", optsDataJson);

        Table mockPartOptsTable = mock(Table.class);
        when(mockPartOptsTable.getItem("healthDataCode", "malformed-data-health-code")).thenReturn(partOptsItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbParticipantOptionsTable(mockPartOptsTable);

        // execute and validate - defaults to no_sharing
        SharingScope sharingScope = helper.getSharingScopeForUser("malformed-data-health-code");
        assertEquals(sharingScope, SharingScope.NO_SHARING);
    }

    @Test
    public void getStudyInfo() {
        // mock DDB Study table - only include relevant attributes
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337)
                .withString("synapseProjectId", "test-synapse-table");

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertEquals(studyInfo.getDataAccessTeamId().longValue(), 1337);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-table");
        assertFalse(studyInfo.getUsesCustomExportSchedule());
        assertFalse(studyInfo.getDisableExport());
    }

    @Test
    public void getStudyInfoDisableExport() {
        // set disable export to false, proceeds as normal
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337)
                .withString("synapseProjectId", "test-synapse-table")
                .withInt(STUDY_DISABLE_EXPORT, 0);

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertEquals(studyInfo.getDataAccessTeamId().longValue(), 1337);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-table");
        assertFalse(studyInfo.getUsesCustomExportSchedule());
        assertFalse(studyInfo.getDisableExport());
    }

    @Test
    public void getStudyInfoEnableExport() {
        // set disable export to true, should have no study info
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337)
                .withString("synapseProjectId", "test-synapse-table")
                .withInt(STUDY_DISABLE_EXPORT, 1);

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertEquals(studyInfo.getDataAccessTeamId().longValue(), 1337);
        assertEquals(studyInfo.getSynapseProjectId(), "test-synapse-table");
        assertFalse(studyInfo.getUsesCustomExportSchedule());
        assertTrue(studyInfo.getDisableExport());
    }

    @Test
    public void getStudyInfoNullDataAccessTeam() {
        // mock DDB Study table - only include relevant attributes
        Item studyItem = new Item().withString("synapseProjectId", "test-synapse-table");

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate - studyInfo is null because the StudyInfo builder returns null if either attributes are
        // null
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertNull(studyInfo);
    }

    @Test
    public void getStudyInfoNullProjectId() {
        // mock DDB Study table - only include relevant attributes
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337);

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate - Similarly, studyInfo is also null here
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertNull(studyInfo);
    }

    @Test
    public void getStudyInfoCustomExportFalse() {
        // mock DDB Study table - only include relevant attributes
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337)
                .withString("synapseProjectId", "test-synapse-table").withInt("usesCustomExportSchedule", 0);

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertFalse(studyInfo.getUsesCustomExportSchedule());
    }

    @Test
    public void getStudyInfoCustomExportTrue() {
        // mock DDB Study table - only include relevant attributes
        Item studyItem = new Item().withLong("synapseDataAccessTeamId", 1337)
                .withString("synapseProjectId", "test-synapse-table").withInt("usesCustomExportSchedule", 1);

        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(studyItem);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertTrue(studyInfo.getUsesCustomExportSchedule());
    }
}
