package org.sagebionetworks.bridge.exporter.dynamo;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper.IDENTIFIER;
import static org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper.LAST_EXPORT_DATE_TIME;
import static org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper.STUDY_DISABLE_EXPORT;
import static org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper.STUDY_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;

@SuppressWarnings("unchecked")
public class DynamoHelperTest {
    private static final String STUDY_TABLE_NAME = "Study";

    private static final String UPLOAD_DATE = "2016-05-09";
    private static final String UPLOAD_START_DATE_TIME = "2016-05-09T00:00:00.000-0700";
    private static final String UPLOAD_END_DATE_TIME = "2016-05-09T23:59:59.999-0700";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);
    private static final LocalDate UPLOAD_DATE_OBJ = LocalDate.parse(UPLOAD_DATE);

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

    @Test
    public void bootstrapStudyIdsToQueryTest() {
        // mock study table and study id list
        Table mockStudyTable = mock(Table.class);
        DynamoScanHelper mockDdbScanHelper = mock(DynamoScanHelper.class);

        Item item1 = new Item().withString(IDENTIFIER, "ddb-foo");
        Item item2 = new Item().withString(IDENTIFIER, "ddb-bar");
        List<Item> studyIdList = ImmutableList.of(item1, item2);
        when(mockDdbScanHelper.scan(any(), any(), eq(IDENTIFIER), any(), any())).thenReturn(studyIdList);

        DynamoHelper dynamoHelper = new DynamoHelper();
        dynamoHelper.setDdbStudyTable(mockStudyTable);
        dynamoHelper.setDdbScanHelper(mockDdbScanHelper);

        // mock request
        BridgeExporterRequest request;
        List<String> studyIdsToUpdate;
        // daily
        request = new BridgeExporterRequest.Builder().withDate(UPLOAD_DATE_OBJ).build();
        studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get(0), "ddb-foo");
        assertEquals(studyIdsToUpdate.get(1), "ddb-bar");

        // with whitelist
        request = new BridgeExporterRequest.Builder().withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withStudyWhitelist(ImmutableSet.of("ddb-foo"))
                .build();

        studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 1);
        assertEquals(studyIdsToUpdate.get(0), "ddb-foo");
    }

    @Test
    public void testModifyExportTimeTable() throws Exception {
        DynamoHelper dynamoHelper = new DynamoHelper();
        Table mockDdbExportTimeTable = mock(Table.class);
        dynamoHelper.setDdbExportTimeTable(mockDdbExportTimeTable);

        List<String> testStudyIdsToUpdate = new ArrayList<>();
        testStudyIdsToUpdate.add("id1");
        testStudyIdsToUpdate.add("id2");

        // execute
        dynamoHelper.updateExportTimeTable(testStudyIdsToUpdate, UPLOAD_END_DATE_TIME_OBJ);

        // verify
        ArgumentCaptor<Item> itemArgumentCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockDdbExportTimeTable, times(2)).putItem(itemArgumentCaptor.capture());

        List<Item> items = itemArgumentCaptor.getAllValues();
        Item item1 = items.get(0);
        assertEquals(item1.get(STUDY_ID), "id1");
        assertEquals(item1.getLong(LAST_EXPORT_DATE_TIME), UPLOAD_END_DATE_TIME_OBJ.getMillis());
        Item item2 = items.get(1);
        assertEquals(item2.get(STUDY_ID), "id2");
        assertEquals(item2.getLong(LAST_EXPORT_DATE_TIME), UPLOAD_END_DATE_TIME_OBJ.getMillis());
    }

    @Test
    public void testNotModifyExportTimeTableEmptyStudyIds() throws Exception {
        DynamoHelper dynamoHelper = new DynamoHelper();
        Table mockDdbExportTimeTable = mock(Table.class);
        dynamoHelper.setDdbExportTimeTable(mockDdbExportTimeTable);

        List<String> testStudyIdsToUpdate = new ArrayList<>();

        // execute
        dynamoHelper.updateExportTimeTable(testStudyIdsToUpdate, UPLOAD_END_DATE_TIME_OBJ);

        // verify
        verifyNoMoreInteractions(mockDdbExportTimeTable);
    }

    @Test
    public void testNotModifyExportTimeTableNullEndDateTime() throws Exception {
        DynamoHelper dynamoHelper = new DynamoHelper();
        Table mockDdbExportTimeTable = mock(Table.class);
        dynamoHelper.setDdbExportTimeTable(mockDdbExportTimeTable);

        List<String> testStudyIdsToUpdate = new ArrayList<>();
        testStudyIdsToUpdate.add("id1");
        testStudyIdsToUpdate.add("id2");

        // execute
        dynamoHelper.updateExportTimeTable(testStudyIdsToUpdate, null);

        // verify
        verifyNoMoreInteractions(mockDdbExportTimeTable);
    }
}
