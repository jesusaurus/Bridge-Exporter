package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.io.File;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class ExportSubtaskTest {
    private static final DateTime DUMMY_REQUEST_DATE_TIME = DateTime.parse("2015-12-06T23:59:59Z");
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withEndDateTime(DUMMY_REQUEST_DATE_TIME).withUseLastExportTime(true).build();
    private static final ExportTask DUMMY_PARENT_TASK = new ExportTask.Builder()
            .withExporterDate(LocalDate.parse("2015-12-07")).withMetrics(new Metrics()).withRequest(DUMMY_REQUEST)
            .withTmpDir(mock(File.class)).build();

    private static final JsonNode DUMMY_RECORD_DATA = DefaultObjectMapper.INSTANCE.createObjectNode();
    private static final UploadSchemaKey DUMMY_SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId("test-study")
            .withSchemaId("test-schema").withRevision(17).build();
    private static final String STUDY_ID = "my-study";

    private static final String DUMMY_RECORD_ID = "dummy-record";
    private static final Item ORIGINAL_RECORD = new Item().withString(ExportSubtask.KEY_RECORD_ID, DUMMY_RECORD_ID);

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "originalRecord must be non-null")
    public void nullOriginalRecord() {
        makeValidSubtaskBuilder().withOriginalRecord(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "parentTask must be non-null")
    public void nullParentTask() {
        makeValidSubtaskBuilder().withParentTask(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "recordData must be non-null")
    public void nullRecordData() {
        makeValidSubtaskBuilder().withRecordData(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "studyId must be specified")
    public void nullStudyId() {
        makeValidSubtaskBuilder().withStudyId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "studyId must be specified")
    public void emptyStudyId() {
        makeValidSubtaskBuilder().withStudyId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "studyId must be specified")
    public void blankStudyId() {
        makeValidSubtaskBuilder().withStudyId("   ").build();
    }

    @Test
    public void happyCase() {
        // build
        ExportSubtask subtask = makeValidSubtaskBuilder().build();

        // validate
        assertSame(subtask.getOriginalRecord(), ORIGINAL_RECORD);
        assertSame(subtask.getParentTask(), DUMMY_PARENT_TASK);
        assertSame(subtask.getRecordData(), DUMMY_RECORD_DATA);
        assertEquals(subtask.getRecordId(), DUMMY_RECORD_ID);
        assertNull(subtask.getSchemaKey());
        assertEquals(subtask.getStudyId(), STUDY_ID);
    }

    @Test
    public void withSchemaKey() {
        // Just validate schema key.
        ExportSubtask subtask = makeValidSubtaskBuilder().withSchemaKey(DUMMY_SCHEMA_KEY).build();
        assertEquals(subtask.getSchemaKey(), DUMMY_SCHEMA_KEY);
    }

    private static ExportSubtask.Builder makeValidSubtaskBuilder() {
        return new ExportSubtask.Builder().withOriginalRecord(ORIGINAL_RECORD).withParentTask(DUMMY_PARENT_TASK)
                .withRecordData(DUMMY_RECORD_DATA).withStudyId(STUDY_ID);
    }
}
