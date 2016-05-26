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
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;
import org.sagebionetworks.bridge.sdk.models.upload.UploadSchema;

/** Synapse export worker for health data tables. */
public class HealthDataExportHandler extends SynapseExportHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataExportHandler.class);

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
    @Cacheable(forever = true)
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();
        for (UploadFieldDefinition oneFieldDef : schema.getFieldDefinitions()) {
            String oneFieldName = oneFieldDef.getName();
            UploadFieldType bridgeType = oneFieldDef.getType();

            ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
            if (synapseType == null) {
                LOG.error("No Synapse type found for Bridge type " + bridgeType);
                synapseType = ColumnType.STRING;
            }

            // hack to cover legacy schemas pre-1k char limit on strings. See comments on
            // shouldConvertFreeformTextToAttachment() for more details.
            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
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
                bridgeType = UploadFieldType.ATTACHMENT_BLOB;
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
