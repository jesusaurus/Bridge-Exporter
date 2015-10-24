package org.sagebionetworks.bridge.exporter.helper;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;

/**
 * This class contains utility methods that call multiple backend services that doesn't really fit into any of the
 * other helpers.
 */
@Component
public class ExportHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ExportHelper.class);

    private static final Map<String, String> SURVEY_TYPE_TO_ANSWER_KEY_MAP = ImmutableMap.<String, String>builder()
            .put("Boolean", "booleanAnswer")
            .put("Date", "dateAnswer")
            .put("Decimal", "numericAnswer")
            .put("Integer", "numericAnswer")
            .put("MultipleChoice", "choiceAnswers")
                    // yes, None really gets the answer from scaleAnswer
            .put("None", "scaleAnswer")
            .put("Scale", "scaleAnswer")
            .put("SingleChoice", "choiceAnswers")
            .put("Text", "textAnswer")
            .put("TimeInterval", "intervalAnswer")
            .put("TimeOfDay", "dateComponentsAnswer")
            .build();

    // config attributes
    private String attachmentBucket;

    // Spring helpers
    private Table ddbAttachmentTable;
    private S3Helper s3Helper;

    @Autowired
    public final void setConfig(Config config) {
        this.attachmentBucket = config.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET);
    }

    @Autowired
    public final void setDdbAttachmentTable(Table ddbAttachmentTable) {
        this.ddbAttachmentTable = ddbAttachmentTable;
    }

    // TODO re-doc
    /** S3 helper. Configured externally. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public JsonNode convertSurveyRecordToHealthDataJsonNode(String recordId, JsonNode oldDataJson,
            UploadSchema surveySchema) throws BridgeExporterException, IOException {
        // download answers from S3 attachments
        JsonNode answerLinkNode = oldDataJson.get("answers");
        if (answerLinkNode == null || answerLinkNode.isNull()) {
            throw new BridgeExporterException("No answer link in survey data");
        }
        String answerLink = answerLinkNode.textValue();
        String answerText = s3Helper.readS3FileAsString(attachmentBucket, answerLink);

        JsonNode answerArrayNode;
        try {
            answerArrayNode = DefaultObjectMapper.INSTANCE.readTree(answerText);
        } catch (IOException ex) {
            throw new BridgeExporterException("Error parsing JSON survey answers from S3 file " + answerLink + ": " +
                    ex.getMessage(), ex);
        }
        if (answerArrayNode == null || answerArrayNode.isNull()) {
            throw new BridgeExporterException("Survey with no answers from S3 file " + answerLink);
        }

        // get schema and field type map, so we can process attachments
        Map<String, String> surveyFieldTypeMap = surveySchema.getFieldTypeMap();

        // copy fields to "non-survey" format
        ObjectNode convertedSurveyNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        int numAnswers = answerArrayNode.size();
        for (int i = 0; i < numAnswers; i++) {
            JsonNode oneAnswerNode = answerArrayNode.get(i);
            if (oneAnswerNode == null || oneAnswerNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has no value");
                continue;
            }

            // question name ("item")
            JsonNode answerItemNode = oneAnswerNode.get("item");
            if (answerItemNode == null || answerItemNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has no question name (item)");
                continue;
            }
            String answerItem = answerItemNode.textValue();
            if (StringUtils.isBlank(answerItem)) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has blank question name (item)");
                continue;
            }

            // question type
            JsonNode questionTypeNameNode = oneAnswerNode.get("questionTypeName");
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                // fall back to questionType
                questionTypeNameNode = oneAnswerNode.get("questionType");
            }
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has no question type");
                continue;
            }
            String questionTypeName = questionTypeNameNode.textValue();
            if (StringUtils.isBlank(questionTypeName)) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has blank question type");
                continue;
            }

            // answer
            String answerKey = SURVEY_TYPE_TO_ANSWER_KEY_MAP.get(questionTypeName);
            if (answerKey == null) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has unknown question type " +
                        questionTypeName);
                continue;
            }

            JsonNode answerAnswerNode = oneAnswerNode.get(answerKey);
            if (answerAnswerNode != null && !answerAnswerNode.isNull()) {
                // handle attachment types (file handle types)
                String bridgeType = surveyFieldTypeMap.get(answerItem);
                ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
                if (synapseType == ColumnType.FILEHANDLEID) {
                    String attachmentId = uploadFreeformTextAsAttachment(recordId, answerAnswerNode.toString());
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

        return convertedSurveyNode;
    }

    public String uploadFreeformTextAsAttachment(String recordId, String text)
            throws AmazonClientException, IOException {
        // write to health data attachments table to reserve guid
        String attachmentId = UUID.randomUUID().toString();
        Item attachment = new Item();
        attachment.withString("id", attachmentId);
        attachment.withString("recordId", recordId);
        ddbAttachmentTable.putItem(attachment);

        // upload to S3
        s3Helper.writeBytesToS3(attachmentBucket, attachmentId, text.getBytes(Charsets.UTF_8));
        return attachmentId;
    }
}
