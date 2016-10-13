package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.jcabi.aspects.Cacheable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.sagebionetworks.bridge.sdk.models.healthData.RecordExportStatusRequest;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

/** Synapse export worker for health data tables. */
public class HealthDataExportHandler extends SynapseExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataExportHandler.class);

    private static final char MULTI_CHOICE_FIELD_SEPARATOR = '.';
    private static final String OTHER_CHOICE_FIELD_SUFFIX = ".other";
    private static final String TIME_ZONE_FIELD_SUFFIX = ".timezone";
    private static final long TIME_ZONE_FIELD_LENGTH = 5;
    private static final DateTimeFormatter TIME_ZONE_FORMATTER = DateTimeFormat.forPattern("Z");
    private static final String TIME_ZONE_UTC_STRING = "+0000";
    private BridgeHelper bridgeHelper = new BridgeHelper();

    private UploadSchemaKey schemaKey;

    /**
     * Schema that this handler represents. This is used for determining the table keys in DDB as well as determining
     * the Synapse table columns and corresponding TSV columns.
     */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    /** @see #getSchemaKey */
    public final void setSchemaKey(UploadSchemaKey schemaKey) {
        this.schemaKey = schemaKey;
    }

    @Override
    protected String getDdbTableName() {
        return "SynapseTables";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "schemaKey";
    }

    @Override
    protected String getDdbTableKeyValue() {
        return schemaKey.toString();
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList(ExportTask task) throws SchemaNotFoundException {
        List<UploadFieldDefinition> schemaFieldDefList = getSchemaFieldDefList(task.getMetrics());
        return getSynapseTableColumnListCached(schemaFieldDefList);
    }

    @Override
    public void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    // Helper method to compute the Synapse column list from the schema field def list. Since this is a non-trivial
    // amount of computation, we also wrap this in a cacheable annotation.
    @Cacheable(forever = true)
    private List<ColumnModel> getSynapseTableColumnListCached(List<UploadFieldDefinition> schemaFieldDefList) {
        List<ColumnModel> columnList = new ArrayList<>();
        for (UploadFieldDefinition oneFieldDef : schemaFieldDefList) {
            String oneFieldName = oneFieldDef.getName();
            UploadFieldType bridgeType = oneFieldDef.getType();

            if (bridgeType == UploadFieldType.MULTI_CHOICE) {
                // Multi-Choice questions create a boolean column for each possible answer in the multi-choice answer
                // list. For example, if Multi-Choice field "sports" has possible answers "fencing", "football",
                // "running", and "swimming", then we create boolean columns "sports.fencing", "sports.football",
                // "sports.running", and "sports.swimming".
                for (String oneAnswer : oneFieldDef.getMultiChoiceAnswerList()) {
                    ColumnModel oneColumn = new ColumnModel();
                    oneColumn.setName(oneFieldName + MULTI_CHOICE_FIELD_SEPARATOR + oneAnswer);
                    oneColumn.setColumnType(ColumnType.BOOLEAN);
                    columnList.add(oneColumn);
                }

                // If this field allows other, we also include the "[name].other" column. Since there's no way to set
                // max length for multi-choice questions, it could be unbounded, so use LargeText.
                if (Boolean.TRUE.equals(oneFieldDef.getAllowOtherChoices())) {
                    ColumnModel otherColumn = new ColumnModel();
                    otherColumn.setName(oneFieldName + OTHER_CHOICE_FIELD_SUFFIX);
                    otherColumn.setColumnType(ColumnType.LARGETEXT);
                    columnList.add(otherColumn);
                }
            } else if (bridgeType == UploadFieldType.TIMESTAMP) {
                // Timestamps have 2 columns, one for the timestamp (as a Synapse DATE field) and one for the timezone.
                ColumnModel timestampColumn = new ColumnModel();
                timestampColumn.setName(oneFieldName);
                timestampColumn.setColumnType(ColumnType.DATE);
                columnList.add(timestampColumn);

                ColumnModel timezoneColumn = new ColumnModel();
                timezoneColumn.setName(oneFieldName + TIME_ZONE_FIELD_SUFFIX);
                timezoneColumn.setColumnType(ColumnType.STRING);
                timezoneColumn.setMaximumSize(TIME_ZONE_FIELD_LENGTH);
                columnList.add(timezoneColumn);
            } else {
                ColumnModel oneColumn = new ColumnModel();
                oneColumn.setName(oneFieldName);

                // Special logic for strings.
                ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
                if (synapseType == ColumnType.STRING) {
                    if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                        // hack to cover legacy schemas pre-1k char limit on strings. See comments on
                        // shouldConvertFreeformTextToAttachment() for more details.
                        oneColumn.setColumnType(ColumnType.FILEHANDLEID);
                    } else {
                        // Could be String or LargeText, depending on max length.
                        Boolean isUnboundedText = oneFieldDef.isUnboundedText();
                        int maxLength = SynapseHelper.getMaxLengthForFieldDef(oneFieldDef);
                        if ((isUnboundedText != null && isUnboundedText) || maxLength > 1000) {
                            oneColumn.setColumnType(ColumnType.LARGETEXT);
                        } else {
                            oneColumn.setColumnType(ColumnType.STRING);
                            oneColumn.setMaximumSize((long) maxLength);
                        }
                    }
                } else {
                    oneColumn.setColumnType(synapseType);
                }

                columnList.add(oneColumn);
            }
        }

        return columnList;
    }

    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return task.getHealthDataTsvInfoForSchema(schemaKey);
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        task.setHealthDataTsvInfoForSchema(schemaKey, tsvInfo);
    }

    @Override
    protected Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws IOException, SchemaNotFoundException,
            SynapseException {
        ExportTask task = subtask.getParentTask();
        JsonNode dataJson = subtask.getRecordData();
        ExportWorkerManager manager = getManager();
        String synapseProjectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        String recordId = subtask.getRecordId();

        // schema-specific columns
        Map<String, String> rowValueMap = new HashMap<>();
        List<UploadFieldDefinition> fieldDefList = getSchemaFieldDefList(task.getMetrics());
        for (UploadFieldDefinition oneFieldDef : fieldDefList) {
            String oneFieldName = oneFieldDef.getName();
            UploadFieldType bridgeType = oneFieldDef.getType();
            JsonNode valueNode = dataJson.get(oneFieldName);

            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                // special hack, see comments on shouldConvertFreeformTextToAttachment()
                // For the purposes of this hack, the only fields in the field def that matter the field name and type
                // (attachment).
                oneFieldDef = new UploadFieldDefinition.Builder().withName(oneFieldName)
                        .withType(UploadFieldType.ATTACHMENT_V2).build();
                if (valueNode != null && !valueNode.isNull() && valueNode.isTextual()) {
                    String attachmentId = manager.getExportHelper().uploadFreeformTextAsAttachment(recordId,
                            valueNode.textValue());
                    valueNode = new TextNode(attachmentId);
                } else {
                    valueNode = null;
                }
            }

            if (bridgeType == UploadFieldType.MULTI_CHOICE) {
                // MULTI_CHOICE serializes into multiple fields. See getSynapseTableColumnList() for details.
                Map<String, String> serializedMultiChoiceFields = serializeMultiChoice(recordId, oneFieldDef,
                        valueNode);
                rowValueMap.putAll(serializedMultiChoiceFields);
            } else if (bridgeType == UploadFieldType.TIMESTAMP) {
                // Similarly, TIMESTAMP serializes into 2 different fields.
                Map<String, String> serializedTimestampFields = serializeTimestamp(recordId, oneFieldName, valueNode);
                rowValueMap.putAll(serializedTimestampFields);
            } else {
                String value = manager.getSynapseHelper().serializeToSynapseType(task.getTmpDir(), synapseProjectId,
                        recordId, oneFieldDef, valueNode);
                rowValueMap.put(oneFieldName, value);
            }
        }

        return rowValueMap;
    }

    /**
     * post process tsv to call update records' exporter status as SUCCEEDED
     * @param tsvInfo
     */
    @Override
    protected void postProcessTsv(TsvInfo tsvInfo) throws BridgeExporterException {
        List<String> recordIds = tsvInfo.getRecordIds();
        if (recordIds == null) {
            throw new BridgeExporterException("No record id in this tsv file.");
        }

        bridgeHelper.updateRecordExporterStatus(recordIds, RecordExportStatusRequest.ExporterStatus.SUCCEEDED);
    }

    // Helper method for getting the field definition list from the schema. This calls through to Bridge using the
    // BridgeHelper, which may cache the schema for a few minutes.
    private List<UploadFieldDefinition> getSchemaFieldDefList(Metrics metrics) throws SchemaNotFoundException {
        UploadSchema schema = getManager().getBridgeHelper().getSchema(metrics, schemaKey);
        return schema.getFieldDefinitions();
    }

    /**
     * <p>
     * Serialize a multi-choice answer to a row value map. The map is a partial map and can be written to a TSV and
     * uploaded to Synapse.
     * </p>
     * <p>
     * Package-scoped to facilitate unit testing.
     * </p>
     *
     * @param fieldDef
     *         field definition, used to get the multi-choice answer list and generate column names
     * @param node
     *         value of the multi-choice answer field
     * @return partial row value map with serialized multi-choice answers
     */
    static Map<String, String> serializeMultiChoice(String recordId, UploadFieldDefinition fieldDef, JsonNode node) {
        if (node == null || node.isNull() || !node.isArray()) {
            // Missing or invalid format. Return empty map (no values).
            return ImmutableMap.of();
        }

        // Determine selected answers. Use TreeSet to maintain answers in a predictable (alphabetical) order.
        int numSelected = node.size();
        Set<String> selectedSet = new TreeSet<>();
        for (int i = 0; i < numSelected; i++) {
            JsonNode oneSelectedNode = node.get(i);
            String oneSelectedAnswer;
            if (oneSelectedNode.isTextual()) {
                // Multi-Choice answers _should_ be strings.
                oneSelectedAnswer = oneSelectedNode.textValue();
            } else {
                // Convert everything else trivially to a string, for robustness.
                oneSelectedAnswer = oneSelectedNode.toString();
            }

            selectedSet.add(oneSelectedAnswer);
        }

        // Write "true" and "false" values based on fieldDef answer list.
        String fieldName = fieldDef.getName();
        Map<String, String> partialValueMap = new HashMap<>();
        for (String oneAnswer : fieldDef.getMultiChoiceAnswerList()) {
            String answerFieldName = fieldName + MULTI_CHOICE_FIELD_SEPARATOR + oneAnswer;
            String answerFieldValue = String.valueOf(selectedSet.contains(oneAnswer));
            partialValueMap.put(answerFieldName, answerFieldValue);

            // Remove the answer from the set, so we can determine leftover answers for "allow other".
            selectedSet.remove(oneAnswer);
        }

        if (!selectedSet.isEmpty()) {
            String otherChoice;
            if (selectedSet.size() == 1) {
                otherChoice = Iterables.getOnlyElement(selectedSet);
            } else {
                otherChoice = BridgeExporterUtil.COMMA_SPACE_JOINER.join(selectedSet);
                LOG.error("Multiple other choices " + otherChoice + " for field " + fieldName + " record " + recordId);
            }

            if (Boolean.TRUE.equals(fieldDef.getAllowOtherChoices())) {
                partialValueMap.put(fieldName + OTHER_CHOICE_FIELD_SUFFIX, otherChoice);
            } else {
                LOG.error("Unknown choice(s) " + otherChoice + " for field " + fieldName + " record " + recordId);
            }
        }

        return partialValueMap;
    }

    /**
     * <p>
     * Serialize a timestamp to a row value map that can be written to a TSV and uploaded to Synapse. The map is a
     * partial map. The keys are the column names and the values are the row values.
     * </p>
     * <p>
     * Package-scoped to facilitate unit testing.
     * </p>
     *
     * @param recordId
     *         record ID that corresponds to the row data, used for logging
     * @param fieldName
     *         name of timestamp field
     * @param node
     *         value of timestamp field
     * @return partial row value map with serialized timestamp
     */
    static Map<String, String> serializeTimestamp(String recordId, String fieldName, JsonNode node) {
        if (node != null && !node.isNull()) {
            if (node.isTextual()) {
                // Timestamp in ISO format. Parse using Joda.
                String timestampString = node.textValue();
                try {
                    DateTime dateTime = DateTime.parse(timestampString);
                    String epochMillisString = String.valueOf(dateTime.getMillis());
                    String timeZoneString = TIME_ZONE_FORMATTER.print(dateTime);
                    return ImmutableMap.<String, String>builder().put(fieldName, epochMillisString)
                            .put(fieldName + TIME_ZONE_FIELD_SUFFIX, timeZoneString).build();
                } catch (IllegalArgumentException ex) {
                    // log an error, but throw out malformatted dates
                    LOG.error("Invalid timestamp " + timestampString + " for record ID " + recordId);
                }
            } else if (node.isNumber()) {
                // Timestamp is epoch milliseconds. Push this straight across as the timestamp. The timezone is UTC
                // ("+0000").
                String epochMillisString = String.valueOf(node.longValue());
                return ImmutableMap.<String, String>builder().put(fieldName, epochMillisString)
                        .put(fieldName + TIME_ZONE_FIELD_SUFFIX, TIME_ZONE_UTC_STRING).build();
            }
        }

        return ImmutableMap.of();
    }
}
