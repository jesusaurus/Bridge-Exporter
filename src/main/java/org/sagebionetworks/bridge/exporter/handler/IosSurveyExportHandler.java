package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * This class is responsible for converting the raw survey answers into a form the Synapse Export Worker can consume.
 * Because this doesn't create Synapse tables, rather only converting the data and forwarding it to other worker
 * threads, this worker has no init and no cleanup, only the worker loop.
 */
public class IosSurveyExportHandler extends ExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IosSurveyExportHandler.class);

    @Override
    public void handle(ExportSubtask subtask) {
        Metrics metrics = subtask.getParentTask().getMetrics();
        Item record = subtask.getOriginalRecord();
        String recordId = record.getString("id");
        String studyId = getStudyId();

        try {
            processRecordAsSurvey(subtask);
            metrics.incrementCounter("surveyWorker[" + studyId + "].surveyCount");
        } catch (BridgeExporterException | IOException | RuntimeException | SchemaNotFoundException ex) {
            metrics.incrementCounter("surveyWorker[" + studyId + "].errorCount");
            LOG.error("Error processing survey record " + recordId + " for study " + studyId + ": " + ex.getMessage(),
                    ex);
        }
    }

    private void processRecordAsSurvey(ExportSubtask subtask) throws BridgeExporterException, IOException,
            SchemaNotFoundException {
        ExportTask parentTask = subtask.getParentTask();
        Item record = subtask.getOriginalRecord();
        String recordId = record.getString("id");

        // get data node and item
        JsonNode oldDataJson = subtask.getRecordData();
        JsonNode itemNode = oldDataJson.get("item");
        if (itemNode == null || itemNode.isNull()) {
            throw new BridgeExporterException("No item field in survey data");
        }
        String item = itemNode.textValue();
        if (StringUtils.isBlank(item)) {
            throw new BridgeExporterException("Null or empty item field in survey data");
        }

        // what schema should we forward this to?
        @SuppressWarnings("UnnecessaryLocalVariable")
        String schemaId = item;
        int schemaRev = 1;
        UploadSchemaKey surveySchemaKey = new UploadSchemaKey.Builder().withStudyId(getStudyId())
                .withSchemaId(schemaId).withRevision(schemaRev).build();

        // get schema and field type map, so we can process attachments
        UploadSchema surveySchema = getManager().getDynamoHelper().getSchema(parentTask.getMetrics(), surveySchemaKey);

        // convert to health data node
        JsonNode convertedSurveyNode = getManager().getExportHelper().convertSurveyRecordToHealthDataJsonNode(recordId,
                oldDataJson, surveySchema);

        ExportSubtask convertedSubtask = new ExportSubtask.Builder().withOriginalRecord(record)
                .withParentTask(parentTask).withRecordData(convertedSurveyNode).withSchemaKey(surveySchemaKey).build();
        getManager().addHealthDataSubtask(parentTask, getStudyId(), surveySchemaKey, convertedSubtask);
    }
}
