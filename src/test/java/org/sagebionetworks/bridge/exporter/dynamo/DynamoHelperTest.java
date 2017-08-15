package org.sagebionetworks.bridge.exporter.dynamo;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

@SuppressWarnings("unchecked")
public class DynamoHelperTest {
    private static final String START_DATE_TIME_STRING = "2016-05-08T11:21:19.004-0700";
    private static final DateTime START_DATE_TIME = DateTime.parse(START_DATE_TIME_STRING);

    private static final String END_DATE_TIME_STRING = "2016-05-09T23:37:44.326-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STRING);

    private static final String FOO_LAST_EXPORT_TIME_STRING = "2016-05-09T20:25:31.346-0700";
    private static final DateTime FOO_LAST_EXPORT_TIME = DateTime.parse(FOO_LAST_EXPORT_TIME_STRING);

    private static final String BAR_LAST_EXPORT_TIME_STRING = "2016-05-09T13:32:46.695-0700";
    private static final DateTime BAR_LAST_EXPORT_TIME = DateTime.parse(BAR_LAST_EXPORT_TIME_STRING);

    private static final String CALCULATED_LAST_EXPORT_TIME_STRING = "2016-05-08T00:00:00.000-0700";
    private static final DateTime CALCULATED_LAST_EXPORT_TIME = DateTime.parse(CALCULATED_LAST_EXPORT_TIME_STRING);

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
    public void getStudyInfoNullStudy() {
        // mock DDB Study table
        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(null);

        // set up Dynamo Helper
        DynamoHelper helper = new DynamoHelper();
        helper.setDdbStudyTable(mockStudyTable);

        // execute and validate - should return null instead of crashing
        StudyInfo studyInfo = helper.getStudyInfo("test-study");
        assertNull(studyInfo);
    }

    @Test
    public void getStudyInfoDisableExportFalse() {
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
    public void getStudyInfoDisableExportTrue() {
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
    public void bootstrapStudyIdsToQueryTestS3Override() throws Exception {
        DynamoHelper dynamoHelper = new DynamoHelper();
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withRecordIdS3Override("test-override")
                .withUseLastExportTime(false).build();
        Map<String, DateTime> retStudyIds = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(retStudyIds.size(), 0);
    }

    @Test
    public void bootstrapStudyIdsToQueryTestNormal() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        Item fooItem = new Item().withString(STUDY_ID, "ddb-foo").withLong(
                LAST_EXPORT_DATE_TIME, FOO_LAST_EXPORT_TIME.getMillis());
        Item barItem = new Item().withString(STUDY_ID, "ddb-bar").withLong(
                LAST_EXPORT_DATE_TIME, BAR_LAST_EXPORT_TIME.getMillis());
        when(mockExportTimeTable.getItem(STUDY_ID, "ddb-foo")).thenReturn(fooItem);
        when(mockExportTimeTable.getItem(STUDY_ID, "ddb-bar")).thenReturn(barItem);

        // Spy DynamoHelper, so we can mock getStudyInfo() and not entwine the implementation of that with this test.
        DynamoHelper dynamoHelper = spy(new DynamoHelper());
        dynamoHelper.setDdbExportTimeTable(mockExportTimeTable);
        dynamoHelper.setConfig(mockConfig());
        mockDdbScanHelperWithStudies(dynamoHelper, "ddb-foo", "ddb-bar", "unconfigured-study", "disabled-study",
                "custom-export-study");

        // mock getStudyInfo()
        doReturn(new StudyInfo.Builder().withSynapseProjectId("foo-synapse-project").withDataAccessTeamId(1111L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-foo");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("bar-synapse-project").withDataAccessTeamId(2222L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-bar");
        doReturn(null).when(dynamoHelper).getStudyInfo("unconfigured-study");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("disabled-project").withDataAccessTeamId(4444L)
                .withDisableExport(true).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("disabled-study");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("custom-export-project").withDataAccessTeamId(5555L)
                .withDisableExport(false).withUsesCustomExportSchedule(true).build())
                .when(dynamoHelper).getStudyInfo("custom-export-study");

        // mock request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withUseLastExportTime(true).build();
        Map<String, DateTime> studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get("ddb-foo").toString(), FOO_LAST_EXPORT_TIME.toString());
        assertEquals(studyIdsToUpdate.get("ddb-bar").toString(), BAR_LAST_EXPORT_TIME.toString());
    }

    @Test
    public void bootstrapStudyIdsToQueryTestWithWhitelist() throws Exception {
        // mock study table and study id list
        DynamoScanHelper mockDdbScanHelper = mock(DynamoScanHelper.class);

        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        Item fooItem = new Item().withString(STUDY_ID, "normal-study-foo").withLong(
                LAST_EXPORT_DATE_TIME, FOO_LAST_EXPORT_TIME.getMillis());
        Item barItem = new Item().withString(STUDY_ID, "custom-export-study-bar").withLong(
                LAST_EXPORT_DATE_TIME, BAR_LAST_EXPORT_TIME.getMillis());
        when(mockExportTimeTable.getItem(STUDY_ID, "normal-study-foo")).thenReturn(fooItem);
        when(mockExportTimeTable.getItem(STUDY_ID, "custom-export-study-bar")).thenReturn(barItem);

        // Spy DynamoHelper, so we can mock getStudyInfo() and not entwine the implementation of that with this test.
        DynamoHelper dynamoHelper = spy(new DynamoHelper());
        dynamoHelper.setDdbScanHelper(mockDdbScanHelper);
        dynamoHelper.setDdbExportTimeTable(mockExportTimeTable);
        dynamoHelper.setConfig(mockConfig());

        // mock getStudyInfo()
        doReturn(new StudyInfo.Builder().withSynapseProjectId("normal-synapse-project").withDataAccessTeamId(1111L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("normal-study-foo");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("custom-export-project").withDataAccessTeamId(2222L)
                .withDisableExport(false).withUsesCustomExportSchedule(true).build())
                .when(dynamoHelper).getStudyInfo("custom-export-study-bar");

        // mock request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withUseLastExportTime(true)
                .withStudyWhitelist(ImmutableSet.of("normal-study-foo", "custom-export-study-bar")).build();
        Map<String, DateTime> studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get("normal-study-foo").toString(), FOO_LAST_EXPORT_TIME.toString());
        assertEquals(studyIdsToUpdate.get("custom-export-study-bar").toString(), BAR_LAST_EXPORT_TIME.toString());

        // We never scan the study table.
        verify(mockDdbScanHelper, never()).scan(any());
    }

    @Test
    public void bootstrapStudyIdsToQueryTestWithStartDateTime() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);

        // Spy DynamoHelper, so we can mock getStudyInfo() and not entwine the implementation of that with this test.
        DynamoHelper dynamoHelper = spy(new DynamoHelper());
        dynamoHelper.setDdbExportTimeTable(mockExportTimeTable);
        dynamoHelper.setConfig(mockConfig());
        mockDdbScanHelperWithStudies(dynamoHelper, "ddb-foo", "ddb-bar");

        // mock getStudyInfo()
        doReturn(new StudyInfo.Builder().withSynapseProjectId("foo-synapse-project").withDataAccessTeamId(1111L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-foo");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("bar-synapse-project").withDataAccessTeamId(2222L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-bar");

        // mock request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withStartDateTime(START_DATE_TIME)
                .withEndDateTime(END_DATE_TIME).withUseLastExportTime(false).build();
        Map<String, DateTime> studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get("ddb-foo").toString(), START_DATE_TIME.toString());
        assertEquals(studyIdsToUpdate.get("ddb-bar").toString(), START_DATE_TIME.toString());

        // We never call the export time table
        verify(mockExportTimeTable, never()).getItem(any(), any());
    }

    @Test
    public void bootstrapStudyIdsToQueryTestNullItem() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);

        // Spy DynamoHelper, so we can mock getStudyInfo() and not entwine the implementation of that with this test.
        DynamoHelper dynamoHelper = spy(new DynamoHelper());
        dynamoHelper.setDdbExportTimeTable(mockExportTimeTable);
        dynamoHelper.setConfig(mockConfig());
        mockDdbScanHelperWithStudies(dynamoHelper, "ddb-foo", "ddb-bar");

        // mock getStudyInfo()
        doReturn(new StudyInfo.Builder().withSynapseProjectId("foo-synapse-project").withDataAccessTeamId(1111L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-foo");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("bar-synapse-project").withDataAccessTeamId(2222L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-bar");

        // mock request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withUseLastExportTime(true).build();
        Map<String, DateTime> studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        assertEquals(studyIdsToUpdate.size(), 2);
        assertEquals(studyIdsToUpdate.get("ddb-foo").toString(), CALCULATED_LAST_EXPORT_TIME.toString());
        assertEquals(studyIdsToUpdate.get("ddb-bar").toString(), CALCULATED_LAST_EXPORT_TIME.toString());
    }

    @Test
    public void bootstrapStudyIdsToQueryTestEndDateTimeBeforeLastExportDateTime() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        Item fooItem = new Item().withString(STUDY_ID, "ddb-foo").withLong(
                LAST_EXPORT_DATE_TIME, END_DATE_TIME.getMillis());
        Item barItem = new Item().withString(STUDY_ID, "ddb-bar").withLong(
                LAST_EXPORT_DATE_TIME, END_DATE_TIME.getMillis() + 10000);
        when(mockExportTimeTable.getItem(STUDY_ID, "ddb-foo")).thenReturn(fooItem);
        when(mockExportTimeTable.getItem(STUDY_ID, "ddb-bar")).thenReturn(barItem);

        // Spy DynamoHelper, so we can mock getStudyInfo() and not entwine the implementation of that with this test.
        DynamoHelper dynamoHelper = spy(new DynamoHelper());
        dynamoHelper.setDdbExportTimeTable(mockExportTimeTable);
        dynamoHelper.setConfig(mockConfig());
        mockDdbScanHelperWithStudies(dynamoHelper, "ddb-foo", "ddb-bar");

        // mock getStudyInfo()
        doReturn(new StudyInfo.Builder().withSynapseProjectId("foo-synapse-project").withDataAccessTeamId(1111L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-foo");
        doReturn(new StudyInfo.Builder().withSynapseProjectId("bar-synapse-project").withDataAccessTeamId(2222L)
                .withDisableExport(false).withUsesCustomExportSchedule(false).build())
                .when(dynamoHelper).getStudyInfo("ddb-bar");

        // mock request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withUseLastExportTime(true).build();
        Map<String, DateTime> studyIdsToUpdate = dynamoHelper.bootstrapStudyIdsToQuery(request);

        // should output an empty study ids map since given end date time is before last export date time
        assertEquals(studyIdsToUpdate.size(), 0);
    }

    private static void mockDdbScanHelperWithStudies(DynamoHelper helper, String... studyIdVarArgs) {
        // create mocks
        Table mockStudyTable = mock(Table.class);
        DynamoScanHelper mockDdbScanHelper = mock(DynamoScanHelper.class);

        // create study entries in mock DDB
        List<Item> itemList = new ArrayList<>();
        for (String oneStudyId : studyIdVarArgs) {
            Item item = new Item().withString(IDENTIFIER, oneStudyId);
            itemList.add(item);
        }
        when(mockDdbScanHelper.scan(mockStudyTable)).thenReturn(itemList);

        // add mocks to helper
        helper.setDdbScanHelper(mockDdbScanHelper);
        helper.setDdbStudyTable(mockStudyTable);
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
        dynamoHelper.updateExportTimeTable(testStudyIdsToUpdate, END_DATE_TIME);

        // verify
        ArgumentCaptor<Item> itemArgumentCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockDdbExportTimeTable, times(2)).putItem(itemArgumentCaptor.capture());

        List<Item> items = itemArgumentCaptor.getAllValues();
        Item item1 = items.get(0);
        assertEquals(item1.get(STUDY_ID), "id1");
        assertEquals(item1.getLong(LAST_EXPORT_DATE_TIME), END_DATE_TIME.getMillis());
        Item item2 = items.get(1);
        assertEquals(item2.get(STUDY_ID), "id2");
        assertEquals(item2.getLong(LAST_EXPORT_DATE_TIME), END_DATE_TIME.getMillis());
    }

    @Test
    public void testNotModifyExportTimeTableEmptyStudyIds() throws Exception {
        DynamoHelper dynamoHelper = new DynamoHelper();
        Table mockDdbExportTimeTable = mock(Table.class);
        dynamoHelper.setDdbExportTimeTable(mockDdbExportTimeTable);

        // execute
        dynamoHelper.updateExportTimeTable(ImmutableList.of(), END_DATE_TIME);

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

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET))
                .thenReturn("dummy-override-bucket");
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME)).thenReturn("America/Los_Angeles");
        return mockConfig;
    }
}
