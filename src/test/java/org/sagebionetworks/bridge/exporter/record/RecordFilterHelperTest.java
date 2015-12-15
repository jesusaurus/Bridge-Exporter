package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import org.joda.time.LocalDate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.SharingScope;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterSharingMode;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class RecordFilterHelperTest {
    private static final String DUMMY_HEALTH_CODE = "dummy-health-code";

    @Test
    public void recordMissingSharingScope() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(null);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.SPONSORS_AND_PARTNERS);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[NO_SHARING]"), 0);
        assertEquals(counterMap.count("excluded[NO_SHARING]"), 1);
    }

    @Test
    public void recordMalformedSharingScope() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(null).withString("userSharingScope", "not a valid sharing scope");

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.SPONSORS_AND_PARTNERS);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[NO_SHARING]"), 0);
        assertEquals(counterMap.count("excluded[NO_SHARING]"), 1);
    }

    // This data provider includes all combinations of record sharing scope, using sharing scope, and request sharing
    // mode. (The fifth column is the minimal sharing scope, to help with tests.)
    // Note that we don't need to test missing user participant options, since DynamoHelper takes care of that for us
    // and is tested elsewhere
    @DataProvider(name = "filterBySharingScopeProvider")
    public Object[][] filterBySharingScopeProvider() {
        return new Object[][] {
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS, BridgeExporterSharingMode.ALL,
                        false, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS, BridgeExporterSharingMode.SHARED,
                        true, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
        };
    }

    @Test(dataProvider = "filterBySharingScopeProvider")
    public void filterBySharingScope(SharingScope recordSharingScope, SharingScope userSharingScope,
            BridgeExporterSharingMode requestSharingMode, boolean expected, SharingScope minimalSharingScope) {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withSharingMode(requestSharingMode).build();
        Item record = makeRecord(recordSharingScope);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(userSharingScope);
        assertEquals(helper.shouldExcludeRecord(metrics, request, record), expected);

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[" + minimalSharingScope.name() + "]"), expected ? 0 : 1);
        assertEquals(counterMap.count("excluded[" + minimalSharingScope.name() + "]"), expected ? 1 : 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void recordMissingStudyId() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of("test-study")).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        helper.shouldExcludeRecord(metrics, request, record);
    }

    @Test
    public void studyFilterAccepts() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of("test-study")).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS).withString("studyId", "test-study");

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study]"), 1);
        assertEquals(counterMap.count("excluded[test-study]"), 0);
    }

    @Test
    public void studyFilterExcludes() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of("test-study")).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS).withString("studyId", "excluded-study");

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[excluded-study]"), 0);
        assertEquals(counterMap.count("excluded[excluded-study]"), 1);
    }

    @Test
    public void tableFilterAccepts() {
        // set up inputs
        Metrics metrics = new Metrics();
        UploadSchemaKey acceptedSchemaKey = new UploadSchemaKey.Builder().withStudyId("test-study")
                .withSchemaId("test-schema").withRevision(3).build();
        BridgeExporterRequest request = makeRequestBuilder().withTableWhitelist(ImmutableSet.of(acceptedSchemaKey))
                .build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS).withString("studyId", "test-study")
                .withString("schemaId", "test-schema").withInt("schemaRevision", 3);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study-test-schema-v3]"), 1);
        assertEquals(counterMap.count("excluded[test-study-test-schema-v3]"), 0);
    }

    @Test
    public void tableFilterExcludes() {
        // set up inputs
        Metrics metrics = new Metrics();
        UploadSchemaKey acceptedSchemaKey = new UploadSchemaKey.Builder().withStudyId("test-study")
                .withSchemaId("test-schema").withRevision(3).build();
        BridgeExporterRequest request = makeRequestBuilder().withTableWhitelist(ImmutableSet.of(acceptedSchemaKey))
                .build();

        // same schema, different revision
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS).withString("studyId", "test-study")
                .withString("schemaId", "test-schema").withInt("schemaRevision", 5);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study-test-schema-v5]"), 0);
        assertEquals(counterMap.count("excluded[test-study-test-schema-v5]"), 1);
    }

    private static Item makeRecord(SharingScope recordSharingScope) {
        Item record = new Item().with("healthCode", DUMMY_HEALTH_CODE);
        if (recordSharingScope != null) {
            record.withString("userSharingScope", recordSharingScope.name());
        }
        return record;
    }

    private static RecordFilterHelper makeRecordFilterHelper(SharingScope userSharingScope) {
        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(userSharingScope);

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);
        return helper;
    }

    private static BridgeExporterRequest.Builder makeRequestBuilder() {
        return new BridgeExporterRequest.Builder().withDate(LocalDate.parse("2015-11-05"))
                .withTag("RecordFilterHelperTest");
    }
}
