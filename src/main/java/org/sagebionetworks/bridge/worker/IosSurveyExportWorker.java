package org.sagebionetworks.bridge.worker;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * This class is responsible for converting the raw survey answers into a form the Synapse Export Worker can consume.
 * Because this doesn't create Synapse tables, rather only converting the data and forwarding it to other worker
 * threads, this worker has no init and no cleanup, only the worker loop.
 */
// TODO: move this logic to the Bridge server
public class IosSurveyExportWorker extends ExportWorker {
    private int errorCount = 0;
    private int surveyCount = 0;

    @Override
    public void run() {
        try {
            while (true) {
                // take waits for an element to become available
                ExportTask task;
                try {
                    task = takeTask();
                } catch (InterruptedException ex) {
                    // noop
                    continue;
                }

                Item record = task.getRecord();
                String recordId = record != null ? record.getString("id") : null;

                try {
                    // We only accept PROCESS_RECORD and END_OF_STREAM tasks. PROCESS_IOS_SURVEY is only generated by
                    // this class.
                    switch (task.getType()) {
                        case PROCESS_RECORD:
                            surveyCount++;
                            processRecordAsSurvey(task);
                            break;
                        case END_OF_STREAM:
                            // END_OF_STREAM means we're finished
                            return;
                        default:
                            errorCount++;
                            System.out.println("Unknown task type " + task.getType().name() + " for record " + recordId
                                    + " for survey worker for study " + getStudyId());
                            break;
                    }
                } catch (SchemaNotFoundException ex) {
                    errorCount++;
                    System.out.println("Schema not found for record " + recordId + " for study " + getStudyId()
                            + ": " + ex.getMessage());
                } catch (ExportWorkerException ex) {
                    errorCount++;
                    System.out.println("Error processing record " + recordId + " for survey worker for study "
                            + getStudyId() + ": " + ex.getMessage());
                } catch (RuntimeException ex) {
                    errorCount++;
                    System.out.println("RuntimeException processing record " + recordId
                            + " for survey worker for study " + getStudyId() + ": " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }
        } catch (Throwable t) {
            System.out.println("Unknown error for survey worker for study " + getStudyId() + ": " + t.getMessage());
            t.printStackTrace(System.out);
        } finally {
            System.out.println("Survey worker for study " + getStudyId() + " done: "
                    + BridgeExporterUtil.getCurrentLocalTimestamp());
        }
    }

    private void processRecordAsSurvey(ExportTask task) throws ExportWorkerException, SchemaNotFoundException {
        // validate record
        Item record = task.getRecord();
        if (record == null) {
            throw new ExportWorkerException("Null record for HealthDataExportWorker");
        }
        String recordId = record.getString("id");

        // get data node and item
        JsonNode oldDataJson;
        try {
            oldDataJson = BridgeExporterUtil.JSON_MAPPER.readTree(record.getString("data"));
        } catch (IOException ex) {
            throw new ExportWorkerException("Error parsing JSON data: " + ex.getMessage(), ex);
        }
        if (oldDataJson == null || oldDataJson.isNull()) {
            throw new ExportWorkerException("No JSON data");
        }
        JsonNode itemNode = oldDataJson.get("item");
        if (itemNode == null || itemNode.isNull()) {
            throw new ExportWorkerException("No item field in survey data");
        }
        String item = itemNode.textValue();
        if (Strings.isNullOrEmpty(item)) {
            throw new ExportWorkerException("Null or empty item field in survey data");
        }

        // what schema should we forward this to?
        String schemaId = item;
        int schemaRev = 1;
        UploadSchemaKey surveySchemaKey = new UploadSchemaKey(getStudyId(), schemaId, schemaRev);

        // download answers from S3 attachments
        JsonNode answerLinkNode = oldDataJson.get("answers");
        if (answerLinkNode == null || answerLinkNode.isNull()) {
            throw new ExportWorkerException("No answer link in survey data");
        }
        String answerLink = answerLinkNode.textValue();
        String answerText;
        try {
            answerText = getManager().getS3Helper().readS3FileAsString(BridgeExporterUtil.S3_BUCKET_ATTACHMENTS,
                    answerLink);
        } catch (AmazonClientException | IOException ex) {
            throw new ExportWorkerException("Error getting survey answers from S3 file " + answerLink + ": "
                    + ex.getMessage(), ex);
        }

        JsonNode answerArrayNode;
        try {
            answerArrayNode = BridgeExporterUtil.JSON_MAPPER.readTree(answerText);
        } catch (IOException ex) {
            throw new ExportWorkerException("Error parsing JSON survey answers from S3 file " + answerLink + ": "
                    + ex.getMessage(), ex);
        }
        if (answerArrayNode == null || answerArrayNode.isNull()) {
            throw new ExportWorkerException("Survey with no answers from S3 file " + answerLink);
        }

        // get schema and field type map, so we can process attachments
        UploadSchema surveySchema = getManager().getSchemaHelper().getSchema(surveySchemaKey);
        Map<String, String> surveyFieldTypeMap = surveySchema.getFieldTypeMap();

        // copy fields to "non-survey" format
        ObjectNode convertedSurveyNode = BridgeExporterUtil.JSON_MAPPER.createObjectNode();
        int numAnswers = answerArrayNode.size();
        for (int i = 0; i < numAnswers; i++) {
            JsonNode oneAnswerNode = answerArrayNode.get(i);
            if (oneAnswerNode == null || oneAnswerNode.isNull()) {
                System.out.println("Survey record ID " + recordId + " answer " + i + " has no value");
                continue;
            }

            // question name ("item")
            JsonNode answerItemNode = oneAnswerNode.get("item");
            if (answerItemNode == null || answerItemNode.isNull()) {
                System.out.println("Survey record ID " + recordId + " answer " + i
                        + " has no question name (item)");
                continue;
            }
            String answerItem = answerItemNode.textValue();
            if (Strings.isNullOrEmpty(answerItem)) {
                System.out.println("Survey record ID " + recordId + " answer " + i
                        + " has null or empty question name (item)");
                continue;
            }

            // question type
            JsonNode questionTypeNameNode = oneAnswerNode.get("questionTypeName");
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                // fall back to questionType
                questionTypeNameNode = oneAnswerNode.get("questionType");
            }
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                System.out.println(
                        "Survey record ID " + recordId + " answer " + i + " has no question type");
                continue;
            }
            String questionTypeName = questionTypeNameNode.textValue();
            if (Strings.isNullOrEmpty(questionTypeName)) {
                System.out.println("Survey record ID " + recordId + " answer " + i
                        + " has null or empty question type");
                continue;
            }

            // answer
            // TODO: Hey, this should really be a Map<String, String>, not a big switch statement
            JsonNode answerAnswerNode = null;
            switch (questionTypeName) {
                case "Boolean":
                    answerAnswerNode = oneAnswerNode.get("booleanAnswer");
                    break;
                case "Date":
                    answerAnswerNode = oneAnswerNode.get("dateAnswer");
                    break;
                case "Decimal":
                case "Integer":
                    answerAnswerNode = oneAnswerNode.get("numericAnswer");
                    break;
                case "MultipleChoice":
                case "SingleChoice":
                    answerAnswerNode = oneAnswerNode.get("choiceAnswers");
                    break;
                case "None":
                case "Scale":
                    // yes, None really gets the answer from scaleAnswer
                    answerAnswerNode = oneAnswerNode.get("scaleAnswer");
                    break;
                case "Text":
                    answerAnswerNode = oneAnswerNode.get("textAnswer");
                    break;
                case "TimeInterval":
                    answerAnswerNode = oneAnswerNode.get("intervalAnswer");
                    break;
                case "TimeOfDay":
                    answerAnswerNode = oneAnswerNode.get("dateComponentsAnswer");
                    break;
                default:
                    System.out.println("Survey record ID " + recordId + " answer " + i
                            + " has unknown question type " + questionTypeName);
                    break;
            }

            if (answerAnswerNode != null && !answerAnswerNode.isNull()) {
                // handle attachment types (file handle types)
                String bridgeType = surveyFieldTypeMap.get(answerItem);
                ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
                if (synapseType == ColumnType.FILEHANDLEID) {
                    String attachmentId = null;
                    String answer = answerAnswerNode.toString();
                    try {
                        attachmentId = uploadFreeformTextAsAttachment(recordId, answer);
                    } catch (AmazonClientException | IOException ex) {
                        System.out.println("Error uploading freeform text as attachment for record ID "
                                + recordId + ", survey item " + answerItem + ": " + ex.getMessage());
                    }

                    convertedSurveyNode.put(answerItem, attachmentId);
                } else {
                    convertedSurveyNode.set(answerItem, answerAnswerNode);
                }
            }

            // if there's a unit, add it as well
            JsonNode unitNode = oneAnswerNode.get("unit");
            if (unitNode != null && !unitNode.isNull()) {
                convertedSurveyNode.set(answerItem + "_unit", unitNode);
            }
        }

        ExportTask convertedTask = new ExportTask(ExportTaskType.PROCESS_IOS_SURVEY, record, convertedSurveyNode);
        getManager().addHealthDataExportTask(surveySchemaKey, convertedTask);
    }

    // TODO: This is copy-pasted from HealthDataExportWorker. This should be refactored.
    private String uploadFreeformTextAsAttachment(String recordId, String text)
            throws AmazonClientException, IOException {
        // write to health data attachments table to reserve guid
        String attachmentId = UUID.randomUUID().toString();
        Item attachment = new Item();
        attachment.withString("id", attachmentId);
        attachment.withString("recordId", recordId);

        Table attachmentsTable = getManager().getDdbClient().getTable("prod-heroku-HealthDataAttachment");
        attachmentsTable.putItem(attachment);

        // upload to S3
        getManager().getS3Helper().writeBytesToS3(BridgeExporterUtil.S3_BUCKET_ATTACHMENTS, attachmentId,
                text.getBytes(Charsets.UTF_8));
        return attachmentId;
    }

    @Override
    protected void reportMetrics() {
        System.out.println("surveyWorker[" + getStudyId() + "].surveyCount: " + surveyCount);
        System.out.println("surveyWorker[" + getStudyId() + "].errorCount: " + errorCount);
    }
}
