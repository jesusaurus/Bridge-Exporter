package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;

import org.sagebionetworks.bridge.synapse.SynapseHelper;

// TODO: unspaghetti this code and move Synapse stuff to SynapseHelper
public class UploadSchemaHelper {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Map<UploadSchemaKey, UploadSchema> schemaMap = new HashMap<>();
    private final Map<UploadSchemaKey, String> synapseTableIdMap = new HashMap<>();

    private Map<String, String> projectIdsByStudy;
    private Table schemaTable;
    private SynapseClient synapseClient;
    private SynapseHelper synapseHelper;
    private Table synapseTablesTable;

    public void setSchemaTable(Table schemaTable) {
        this.schemaTable = schemaTable;
    }

    public void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    public void setSynapseTablesTable(Table synapseTablesTable) {
        this.synapseTablesTable = synapseTablesTable;
    }

    public void init() throws IOException {
        // schemas
        Iterable<Item> schemaIter = schemaTable.scan();
        for (Item oneDdbSchema : schemaIter) {
            String schemaTableKey = oneDdbSchema.getString("key");
            String[] parts = schemaTableKey.split(":", 2);
            String studyId = parts[0];
            String schemaId = parts[1];

            int schemaRev = oneDdbSchema.getInt("revision");
            UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);
            UploadSchema schema = UploadSchema.fromDdbItem(schemaKey, oneDdbSchema);

            schemaMap.put(schemaKey, schema);
        }

        // synapse config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        JsonNode synapseConfigJson = JSON_MAPPER.readTree(synapseConfigFile);
        String synapseUserName = synapseConfigJson.get("username").textValue();
        String synapseApiKey = synapseConfigJson.get("apiKey").textValue();

        // synapse client
        synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(synapseUserName);
        synapseClient.setApiKey(synapseApiKey);

        // project ID mapping
        projectIdsByStudy = new HashMap<>();
        JsonNode projectIdMapNode = synapseConfigJson.get("projectIdsByStudy");
        Iterator<String> projectIdMapKeyIter = projectIdMapNode.fieldNames();
        while (projectIdMapKeyIter.hasNext()) {
            String oneStudyId = projectIdMapKeyIter.next();
            projectIdsByStudy.put(oneStudyId, projectIdMapNode.get(oneStudyId).textValue());
        }
    }

    public UploadSchema getSchema(UploadSchemaKey schemaKey) {
        return schemaMap.get(schemaKey);
    }

    public String getSynapseTableId(UploadSchemaKey schemaKey) throws SynapseException {
        // check cache
        String synapseTableId = synapseTableIdMap.get(schemaKey);
        if (synapseTableId != null) {
            return synapseTableId;
        }

        // check DDB table
        Iterable<Item> tableIterable = synapseTablesTable.query("schemaKey", schemaKey.toString());
        Iterator<Item> tableIterator = tableIterable.iterator();
        if (tableIterator.hasNext()) {
            Item oneTable = tableIterator.next();
            synapseTableId = oneTable.getString("tableId");
            synapseTableIdMap.put(schemaKey, synapseTableId);
            return synapseTableId;
        }

        // create columns
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
        UploadSchema schema = getSchema(schemaKey);
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
            if (BridgeExporter.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
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

        // create columns
        List<ColumnModel> createdColumnList = synapseClient.createColumnModels(columnList);
        if (columnList.size() != createdColumnList.size()) {
            throw new RuntimeException("Tried to create " + columnList.size() + " columns. Actual: "
                    + createdColumnList.size() + " columns.");
        }

        List<String> columnIdList = new ArrayList<>();
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            columnIdList.add(oneCreatedColumn.getId());
        }

        // create table
        TableEntity synapseTable = new TableEntity();
        synapseTable.setName(schemaKey.toString());
        synapseTable.setParentId(projectIdsByStudy.get(schemaKey.getStudyId()));
        synapseTable.setColumnIds(columnIdList);
        TableEntity createdTable = synapseClient.createEntity(synapseTable);
        synapseTableId = createdTable.getId();

        synapseHelper.createAclsForTableInStudy(schemaKey.getStudyId(), synapseTableId);

        // write back to DDB table
        Item synapseTableDdbItem = new Item();
        synapseTableDdbItem.withString("schemaKey", schemaKey.toString());
        synapseTableDdbItem.withString("tableId", synapseTableId);
        synapseTablesTable.putItem(synapseTableDdbItem);

        // write back to cache
        synapseTableIdMap.put(schemaKey, synapseTableId);

        return synapseTableId;
    }
}
