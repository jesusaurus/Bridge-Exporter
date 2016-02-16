package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jcabi.aspects.Cacheable;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.schema.UploadFieldTypes;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

/** Synapse export worker for health data tables. */
public class HealthDataExportHandler extends SynapseExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataExportHandler.class);

    private UploadSchema schema;

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
        return schema.getKey().toString();
    }

    @Override
    @Cacheable(forever = true)
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);

            ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
            if (synapseType == null) {
                LOG.error("No Synapse type found for Bridge type " + bridgeType);
                synapseType = ColumnType.STRING;
            }

            // hack to cover legacy schemas pre-1k char limit on strings. See comments on
            // shouldConvertFreeformTextToAttachment() for more details.
            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schema.getKey(), oneFieldName)) {
                synapseType = ColumnType.FILEHANDLEID;
            }

            ColumnModel oneColumn = new ColumnModel();
            oneColumn.setName(oneFieldName);
            oneColumn.setColumnType(synapseType);

            if (synapseType == ColumnType.STRING) {
                Integer maxLength = SynapseHelper.BRIDGE_TYPE_TO_MAX_LENGTH.get(bridgeType);
                if (maxLength == null) {
                    LOG.error("No max length found for Bridge type " + bridgeType);
                    maxLength = SynapseHelper.DEFAULT_MAX_LENGTH;
                }
                oneColumn.setMaximumSize(Long.valueOf(maxLength));
            }

            columnList.add(oneColumn);
        }

        return columnList;
    }

    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return task.getHealthDataTsvInfoForSchema(schema.getKey());
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        task.setHealthDataTsvInfoForSchema(schema.getKey(), tsvInfo);
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
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);
            JsonNode valueNode = dataJson.get(oneFieldName);

            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schema.getKey(), oneFieldName)) {
                // special hack, see comments on shouldConvertFreeformTextToAttachment()
                bridgeType = UploadFieldTypes.ATTACHMENT_BLOB;
                if (valueNode != null && !valueNode.isNull() && valueNode.isTextual()) {
                    String attachmentId = manager.getExportHelper().uploadFreeformTextAsAttachment(recordId,
                            valueNode.textValue());
                    valueNode = new TextNode(attachmentId);
                } else {
                    valueNode = null;
                }
            }

            String value = manager.getSynapseHelper().serializeToSynapseType(task.getTmpDir(), synapseProjectId,
                    recordId, oneFieldName, bridgeType, valueNode);
            rowValueMap.put(oneFieldName, value);
        }

        return rowValueMap;
    }
}
