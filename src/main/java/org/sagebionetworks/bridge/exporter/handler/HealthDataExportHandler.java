package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.jcabi.aspects.Cacheable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final String TIME_ZONE_FIELD_SUFFIX = ".timezone";
    private static final long TIME_ZONE_FIELD_LENGTH = 5;
    private static final DateTimeFormatter TIME_ZONE_FORMATTER = DateTimeFormat.forPattern("Z");
    private static final String TIME_ZONE_UTC_STRING = "+0000";

    private UploadSchema schema;
    private UploadSchemaKey schemaKey;

    /**
     * Schema that this handler represents. This is used for determining the table keys in DDB as well as determining
     * the Synapse table columns and corresponding TSV columns.
     */
    public UploadSchema getSchema() {
        return schema;
    }

    /** @see #getSchema */
    public final void setSchema(UploadSchema schema) {
        this.schema = schema;
        this.schemaKey = BridgeExporterUtil.getSchemaKeyFromSchema(schema);
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
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();
        for (UploadFieldDefinition oneFieldDef : schema.getFieldDefinitions()) {
            String oneFieldName = oneFieldDef.getName();
            UploadFieldType bridgeType = oneFieldDef.getType();

            if (bridgeType == UploadFieldType.MULTI_CHOICE) {
                throw new UnsupportedOperationException("MULTI_CHOICE not yet implemented");
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
                        int maxLength = SynapseHelper.getMaxLengthForFieldDef(oneFieldDef);
                        if (maxLength > 1000) {
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
    protected Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws IOException, SynapseException {
        ExportTask task = subtask.getParentTask();
        JsonNode dataJson = subtask.getRecordData();
        ExportWorkerManager manager = getManager();
        String synapseProjectId = manager.getSynapseProjectIdForStudyAndTask(getStudyId(), task);
        Item record = subtask.getOriginalRecord();
        String recordId = record.getString("id");

        // schema-specific columns
        Map<String, String> rowValueMap = new HashMap<>();
        for (UploadFieldDefinition oneFieldDef : schema.getFieldDefinitions()) {
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
                throw new UnsupportedOperationException("MULTI_CHOICE not yet implemented");
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
