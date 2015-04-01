package org.sagebionetworks.bridge.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/** Synapse export worker for health data tables. */
public class HealthDataExportWorker extends SynapseExportWorker {
    // Configured externally
    private S3Helper s3Helper;
    private UploadSchemaHelper schemaHelper;
    private UploadSchemaKey schemaKey;
    private SynapseHelper synapseHelper;
    private String todaysDateString;

    // Internal state
    private UploadSchema schema;

    /** S3 helper. Configured externally. */
    public S3Helper getS3Helper() {
        return s3Helper;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Schema helper. Configured externally. */
    public UploadSchemaHelper getSchemaHelper() {
        return schemaHelper;
    }

    public void setSchemaHelper(UploadSchemaHelper schemaHelper) {
        this.schemaHelper = schemaHelper;
    }

    /** Schema key corresponding to this health data record table. Configured externally. */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    public void setSchemaKey(UploadSchemaKey schemaKey) {
        this.schemaKey = schemaKey;
    }

    /** Synapse helper. Configured externally. */
    public SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    public void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Calendar date in YYYY-MM-DD format, representing when the data is uploaded to Synapse. (Because uploads can take
     * some time, this corresponds with when the upload started. Ideally, the uploads shouldn't take long enough to
     * cross over to the next day.
     */
    public String getTodaysDateString() {
        return todaysDateString;
    }

    public void setTodaysDateString(String todaysDateString) {
        this.todaysDateString = todaysDateString;
    }

    @Override
    protected void initSchemas() throws SchemaNotFoundException {
        schema = schemaHelper.getSchema(schemaKey);
    }

    @Override
    protected String getDdbTableName() {
        return "SynapseTable";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "schemaKey";
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();

        // common columns
        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnList.add(recordIdColumn);

        ColumnModel healthCodeColumn = new ColumnModel();
        healthCodeColumn.setName("healthCode");
        healthCodeColumn.setColumnType(ColumnType.STRING);
        healthCodeColumn.setMaximumSize(36L);
        columnList.add(healthCodeColumn);

        ColumnModel externalIdColumn = new ColumnModel();
        externalIdColumn.setName("externalId");
        externalIdColumn.setColumnType(ColumnType.STRING);
        externalIdColumn.setMaximumSize(128L);
        columnList.add(externalIdColumn);

        // NOTE: ColumnType.DATE is actually a timestamp. There is no calendar date type.
        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName("uploadDate");
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);
        columnList.add(uploadDateColumn);

        ColumnModel createdOnColumn = new ColumnModel();
        createdOnColumn.setName("createdOn");
        createdOnColumn.setColumnType(ColumnType.DATE);
        columnList.add(createdOnColumn);

        ColumnModel appVersionColumn = new ColumnModel();
        appVersionColumn.setName("appVersion");
        appVersionColumn.setColumnType(ColumnType.STRING);
        appVersionColumn.setMaximumSize(48L);
        columnList.add(appVersionColumn);

        ColumnModel phoneInfoColumn = new ColumnModel();
        phoneInfoColumn.setName("phoneInfo");
        phoneInfoColumn.setColumnType(ColumnType.STRING);
        phoneInfoColumn.setMaximumSize(48L);
        columnList.add(phoneInfoColumn);

        // schema specific columns
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);

            ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
            if (synapseType == null) {
                System.out.println("No Synapse type found for Bridge type " + bridgeType);
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
                    System.out.println("No max length found for Bridge type " + bridgeType);
                    maxLength = 1000;
                }
                oneColumn.setMaximumSize(Long.valueOf(maxLength));
            }

            columnList.add(oneColumn);
        }

        return columnList;
    }

    @Override
    protected String getSynapseTableName() {
        return schemaKey.toString();
    }

    @Override
    protected List<String> getTsvHeaderList() {
        List<String> tsvFieldNameList = new ArrayList<>();

        // common fields
        tsvFieldNameList.add("recordId");
        tsvFieldNameList.add("healthCode");
        tsvFieldNameList.add("externalId");
        tsvFieldNameList.add("uploadDate");
        tsvFieldNameList.add("createdOn");
        tsvFieldNameList.add("appVersion");
        tsvFieldNameList.add("phoneInfo");

        // schema specific fields
        tsvFieldNameList.addAll(schema.getFieldNameList());

        return tsvFieldNameList;
    }

    @Override
    protected List<String> getTsvRowValueList(ExportTask task) throws ExportWorkerException {
        Item record = task.getRecord();
        if (record == null) {
            throw new ExportWorkerException("Null record for HealthDataExportWorker");
        }
        String recordId = record.getString("id");

        // Extract data JSON.
        JsonNode dataJson;
        switch (task.getType()) {
            case PROCESS_RECORD:
                try {
                    dataJson = BridgeExporterUtil.JSON_MAPPER.readTree(record.getString("data"));
                } catch (IOException ex) {
                    throw new ExportWorkerException("Error parsing JSON: " + ex.getMessage(), ex);
                }
                break;
            case PROCESS_IOS_SURVEY:
                dataJson = task.getDataJsonNode();
                break;
            default:
                throw new ExportWorkerException("Invalid task type for HealthDataExportWorker.getTsvRowValueList():"
                        + task.getType().name());
        }
        if (dataJson == null || dataJson.isNull()) {
            throw new ExportWorkerException("Null data JSON node for HealthDataExportWorker");
        }

        // get phone and app info
        String appVersion = null;
        String phoneInfo = null;
        String metadataString = record.getString("metadata");
        if (!Strings.isNullOrEmpty(metadataString)) {
            try {
                JsonNode metadataJson = BridgeExporterUtil.JSON_MAPPER.readTree(metadataString);
                appVersion = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "appVersion", 48,
                        recordId);
                phoneInfo = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "phoneInfo", 48, recordId);
            } catch (IOException ex) {
                // we can recover from this
                System.out.println("Error parsing metadata for record ID " + recordId + ": " + ex.getMessage());
            }
        }

        List<String> rowValueList = new ArrayList<>();

        // common values
        rowValueList.add(recordId);
        rowValueList.add(record.getString("healthCode"));
        rowValueList.add(BridgeExporterUtil.getDdbStringRemoveTabsAndTrim(record, "userExternalId", 128, recordId));
        rowValueList.add(todaysDateString);

        // createdOn as a long epoch millis
        rowValueList.add(String.valueOf(record.getLong("createdOn")));

        rowValueList.add(appVersion);
        rowValueList.add(phoneInfo);

        // schema-specific columns
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);
            JsonNode valueNode = dataJson.get(oneFieldName);

            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                // special hack, see comments on shouldConvertFreeformTextToAttachment()
                bridgeType = "attachment_blob";
                if (valueNode != null && !valueNode.isNull() && valueNode.isTextual()) {
                    try {
                        String attachmentId = uploadFreeformTextAsAttachment(recordId, valueNode.textValue());
                        valueNode = new TextNode(attachmentId);
                    } catch (AmazonClientException | IOException ex) {
                        System.out.println("Error uploading freeform text as attachment for record ID " + recordId
                                + ", field " + oneFieldName + ": " + ex.getMessage());
                        valueNode = null;
                    }
                } else {
                    valueNode = null;
                }
            }

            String value = synapseHelper.serializeToSynapseType(getStudyId(), recordId, oneFieldName, bridgeType,
                    valueNode);
            rowValueList.add(value);
        }

        return rowValueList;
    }

    private String uploadFreeformTextAsAttachment(String recordId, String text)
            throws AmazonClientException, IOException {
        // write to health data attachments table to reserve guid
        String attachmentId = UUID.randomUUID().toString();
        Item attachment = new Item();
        attachment.withString("id", attachmentId);
        attachment.withString("recordId", recordId);

        Table attachmentsTable = getDdbClient().getTable("prod-heroku-HealthDataAttachment");
        attachmentsTable.putItem(attachment);

        // upload to S3
        s3Helper.writeBytesToS3(BridgeExporterUtil.S3_BUCKET_ATTACHMENTS, attachmentId, text.getBytes(Charsets.UTF_8));
        return attachmentId;
    }
}
