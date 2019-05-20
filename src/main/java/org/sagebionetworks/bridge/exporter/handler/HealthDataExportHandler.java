package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.jcabi.aspects.Cacheable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/** Synapse export worker for health data tables. */
public class HealthDataExportHandler extends SynapseExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataExportHandler.class);

    static final String COLUMN_NAME_RAW_DATA = "rawData";
    static final String DDB_KEY_RAW_DATA_ATTACHMENT_ID = "rawDataAttachmentId";
    private static final String METADATA_FIELD_NAME_PREFIX = "metadata.";
    private static final char MULTI_CHOICE_FIELD_SEPARATOR = '.';
    private static final String OTHER_CHOICE_FIELD_SUFFIX = ".other";
    private static final String TIME_ZONE_FIELD_SUFFIX = ".timezone";
    private static final long TIME_ZONE_FIELD_LENGTH = 5;
    private static final DateTimeFormatter TIME_ZONE_FORMATTER = DateTimeFormat.forPattern("Z");
    private static final String TIME_ZONE_UTC_STRING = "+0000";

    static final ColumnModel RAW_DATA_COLUMN;
    static {
        RAW_DATA_COLUMN = new ColumnModel();
        RAW_DATA_COLUMN.setName(COLUMN_NAME_RAW_DATA);
        RAW_DATA_COLUMN.setColumnType(ColumnType.FILEHANDLEID);
    }

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
    protected String getSynapseTableName() {
        return schemaKey.getSchemaId() + "-v" + schemaKey.getRevision();
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList(ExportTask task) throws SchemaNotFoundException {
        List<UploadFieldDefinition> studyUploadMetadataFieldDefList = getStudyUploadMetadataFieldDefList();
        List<UploadFieldDefinition> schemaFieldDefList = getSchemaFieldDefList(task.getMetrics());
        return getSynapseTableColumnListCached(studyUploadMetadataFieldDefList, schemaFieldDefList);
    }

    // Helper method to compute the Synapse column list from the schema field def list. Since this is a non-trivial
    // amount of computation, we also wrap this in a cacheable annotation.
    @Cacheable(forever = true)
    private List<ColumnModel> getSynapseTableColumnListCached(
            List<UploadFieldDefinition> studyUploadMetadataFieldDefList,
            List<UploadFieldDefinition> schemaFieldDefList) {

        // Merge field defs from study upload metadata and from schema.
        List<UploadFieldDefinition> mergedList = mergeFieldDefLists(studyUploadMetadataFieldDefList,
                schemaFieldDefList);

        // Convert into Synapse columms.
        List<ColumnModel> columnList = new ArrayList<>();
        for (UploadFieldDefinition oneFieldDef : mergedList) {
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
                        Boolean isUnboundedText = oneFieldDef.getUnboundedText();
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

        // Add raw data field.
        columnList.add(RAW_DATA_COLUMN);

        return columnList;
    }

    // Helper method that merges the field def list from the study upload metadata and the schema.
    private static List<UploadFieldDefinition> mergeFieldDefLists(
            List<UploadFieldDefinition> studyUploadMetadataFieldDefList,
            List<UploadFieldDefinition> schemaFieldDefList) {
        // Short-cut: If there are no metadata fields, just return the schema field list verbatim.
        if (studyUploadMetadataFieldDefList == null || studyUploadMetadataFieldDefList.isEmpty()) {
            return schemaFieldDefList;
        }

        // Fields from schema overwrite fields from study. To figure this out, we need a hash set to efficiently look
        // up name conflicts.
        Set<String> schemaFieldNameSet = schemaFieldDefList.stream().map(UploadFieldDefinition::getName).collect(
                Collectors.toSet());

        // Only add metadata fields if they aren't in the schema. Note: Metadata fields need to be pre-pended with the
        // prefix.
        List<UploadFieldDefinition> mergedList = new ArrayList<>();
        for (UploadFieldDefinition oneStudyFieldDef : studyUploadMetadataFieldDefList) {
            String resolvedName = METADATA_FIELD_NAME_PREFIX + oneStudyFieldDef.getName();
            if (!schemaFieldNameSet.contains(resolvedName)) {
                // Need to create a copy before renaming the field. Otherwise, this renamed field might get stuck in the
                // cache and then bad things happen.
                UploadFieldDefinition renamedField = copy(oneStudyFieldDef);
                renamedField.setName(resolvedName);
                mergedList.add(renamedField);
            }
        }

        // Append the fields from the schema.
        mergedList.addAll(schemaFieldDefList);
        return mergedList;
    }

    // Helper method that copies an UploadFieldDefinition.
    // Package-scoped to facilitate unit tests.
    static UploadFieldDefinition copy(UploadFieldDefinition other) {
        UploadFieldDefinition copy = new UploadFieldDefinition();
        copy.setName(other.getName());
        copy.setRequired(other.getRequired());
        copy.setType(other.getType());
        copy.setAllowOtherChoices(other.getAllowOtherChoices());
        copy.setFileExtension(other.getFileExtension());
        copy.setMimeType(other.getMimeType());
        copy.setMaxLength(other.getMaxLength());
        copy.setMultiChoiceAnswerList(other.getMultiChoiceAnswerList());
        copy.setUnboundedText(other.getUnboundedText());
        return copy;
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
        ExportWorkerManager manager = getManager();
        ExportTask task = subtask.getParentTask();
        String synapseProjectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        Map<String, String> rowValueMap = new HashMap<>();

        // metadata columns
        String userMetadataJsonText = subtask.getOriginalRecord().getString("userMetadata");
        if (StringUtils.isNotBlank(userMetadataJsonText)) {
            // extract and serialize from the raw DDB record
            JsonNode userMetadataNode = DefaultObjectMapper.INSTANCE.readTree(userMetadataJsonText);
            List<UploadFieldDefinition> metadataFieldDefList = getStudyUploadMetadataFieldDefList();
            if (metadataFieldDefList != null && !metadataFieldDefList.isEmpty()) {
                Map<String, String> metadataFieldMap = extractAndSerializeFields(subtask, metadataFieldDefList,
                        userMetadataNode);

                // Add to the row value map, but pre-pend the metadata prefix.
                metadataFieldMap.forEach((key, value) -> rowValueMap.put(METADATA_FIELD_NAME_PREFIX + key, value));
            }
        }

        // schema-specific columns - These write directly to the rowValueMap, possibly overwriting metadata if there's
        // a name conflict.
        List<UploadFieldDefinition> schemaFieldDefList = getSchemaFieldDefList(task.getMetrics());
        Map<String, String> schemaFieldMap = extractAndSerializeFields(subtask, schemaFieldDefList,
                subtask.getRecordData());
        rowValueMap.putAll(schemaFieldMap);

        // Upload raw data. Attachment ID includes record ID, so we can use it verbatim.
        String rawDataAttachmentId = subtask.getOriginalRecord().getString(DDB_KEY_RAW_DATA_ATTACHMENT_ID);
        if (StringUtils.isNotBlank(rawDataAttachmentId)) {
            String fileHandleId = manager.getSynapseHelper().uploadFromS3ToSynapseFileHandle(synapseProjectId,
                    rawDataAttachmentId);
            rowValueMap.put(COLUMN_NAME_RAW_DATA, fileHandleId);
        }

        return rowValueMap;
    }

    /**
     * Helper method to serialize fields from the given JSON node and return them as a map.
     *
     * @param subtask
     *         export subtask, used for looking up variables and logging info
     * @param fieldDefList
     *         field definition list; either study upload metadata fields or schema fields
     * @param jsonNode
     *         JSON node containing data; either record data or record user metadata
     */
    private Map<String, String> extractAndSerializeFields(ExportSubtask subtask,
            List<UploadFieldDefinition> fieldDefList, JsonNode jsonNode) throws IOException, SynapseException {
        ExportWorkerManager manager = getManager();
        ExportTask task = subtask.getParentTask();
        String synapseProjectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        String recordId = subtask.getRecordId();

        Map<String, String> rowValueMap = new HashMap<>();
        for (UploadFieldDefinition oneFieldDef : fieldDefList) {
            String oneFieldName = oneFieldDef.getName();
            UploadFieldType bridgeType = oneFieldDef.getType();
            JsonNode valueNode = jsonNode.get(oneFieldName);

            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                // special hack, see comments on shouldConvertFreeformTextToAttachment()
                // For the purposes of this hack, the only fields in the field def that matter the field name and type
                // (attachment).
                oneFieldDef = new UploadFieldDefinition().name(oneFieldName).type(UploadFieldType.ATTACHMENT_V2);
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
                String value = manager.getSynapseHelper().serializeToSynapseType(task.getMetrics(), task.getTmpDir(),
                        synapseProjectId, recordId, getStudyId(), oneFieldDef, valueNode);
                rowValueMap.put(oneFieldName, value);
            }
        }

        return rowValueMap;
    }

    /**
     * post process tsv to call update records' exporter status as SUCCEEDED
     */
    @Override
    protected void postProcessTsv(TsvInfo tsvInfo) {
        List<String> recordIds = tsvInfo.getRecordIds();

        getManager().getBridgeHelper().updateRecordExporterStatus(recordIds, SynapseExporterStatus.SUCCEEDED);
    }

    // Helper method for getting the field definition list from the schema. This calls through to Bridge using the
    // BridgeHelper, which may cache the schema for a few minutes.
    private List<UploadFieldDefinition> getSchemaFieldDefList(Metrics metrics) throws SchemaNotFoundException {
        UploadSchema schema = getManager().getBridgeHelper().getSchema(metrics, schemaKey);
        return schema.getFieldDefinitions();
    }

    // Helper method for getting the upload metadata field def list from the study. This is similarly cached in
    // BridgeHelper.
    private List<UploadFieldDefinition> getStudyUploadMetadataFieldDefList() {
        Study study = getManager().getBridgeHelper().getStudy(getStudyId());
        return study.getUploadMetadataFieldDefinitions();
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
                LOG.warn("Multiple other choices " + otherChoice + " for field " + fieldName + " record " + recordId);
            }

            if (Boolean.TRUE.equals(fieldDef.getAllowOtherChoices())) {
                partialValueMap.put(fieldName + OTHER_CHOICE_FIELD_SUFFIX, otherChoice);
            } else {
                LOG.warn("Unknown choice(s) " + otherChoice + " for field " + fieldName + " record " + recordId);
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
                    // log a warning, but throw out malformatted dates
                    LOG.warn("Invalid timestamp " + timestampString + " for record ID " + recordId);
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
