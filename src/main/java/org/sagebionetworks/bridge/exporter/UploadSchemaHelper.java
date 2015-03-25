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
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;

// TODO: unspaghetti this code and move Synapse stuff to SynapseHelper
public class UploadSchemaHelper {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Map<UploadSchemaKey, Item> schemaMap = new HashMap<>();
    private final Map<UploadSchemaKey, String> synapseTableIdMap = new HashMap<>();

    private Map<String, ColumnType> bridgeTypeToSynapseType;
    private Map<String, Integer> bridgeTypeToMaxLength;
    private Map<String, String> projectIdsByStudy;
    private Table schemaTable;
    private SynapseClient synapseClient;
    private Table synapseTablesTable;

    public void setSchemaTable(Table schemaTable) {
        this.schemaTable = schemaTable;
    }

    public void setSynapseTablesTable(Table synapseTablesTable) {
        this.synapseTablesTable = synapseTablesTable;
    }

    public void init() throws IOException {
        // schemas
        Iterable<Item> schemaIter = schemaTable.scan();
        for (Item oneSchema : schemaIter) {
            String schemaTableKey = oneSchema.getString("key");
            String[] parts = schemaTableKey.split(":", 2);
            String studyId = parts[0];
            String schemaId = parts[1];

            int schemaRev = oneSchema.getInt("revision");
            UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

            schemaMap.put(schemaKey, oneSchema);
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

        // type map
        // TODO: use BridgePF enum instead of hardcoded strings
        bridgeTypeToSynapseType = new HashMap<>();
        bridgeTypeToSynapseType.put("attachment_blob", ColumnType.FILEHANDLEID);
        bridgeTypeToSynapseType.put("attachment_csv", ColumnType.FILEHANDLEID);
        bridgeTypeToSynapseType.put("attachment_json_blob", ColumnType.FILEHANDLEID);
        bridgeTypeToSynapseType.put("attachment_json_table", ColumnType.FILEHANDLEID);
        bridgeTypeToSynapseType.put("boolean", ColumnType.BOOLEAN);
        bridgeTypeToSynapseType.put("calendar_date", ColumnType.STRING);
        bridgeTypeToSynapseType.put("float", ColumnType.DOUBLE);
        bridgeTypeToSynapseType.put("inline_json_blob", ColumnType.STRING);
        bridgeTypeToSynapseType.put("int", ColumnType.INTEGER);
        bridgeTypeToSynapseType.put("string", ColumnType.STRING);
        bridgeTypeToSynapseType.put("timestamp", ColumnType.DATE);

        bridgeTypeToMaxLength = new HashMap<>();
        bridgeTypeToMaxLength.put("calendar_date", 10);
        bridgeTypeToMaxLength.put("inline_json_blob", 1000);
        bridgeTypeToMaxLength.put("string", 1000);
    }

    public Item getSchema(UploadSchemaKey schemaKey) {
        return schemaMap.get(schemaKey);
    }

    public String getSynapseTableId(UploadSchemaKey schemaKey) throws IOException, SynapseException {
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
        Item schema = getSchema(schemaKey);
        JsonNode fieldDefList = JSON_MAPPER.readTree(schema.getString("fieldDefinitions"));
        for (JsonNode oneFieldDef : fieldDefList) {
            String name = oneFieldDef.get("name").textValue();
            String bridgeType = oneFieldDef.get("type").textValue().toLowerCase();

            ColumnType synapseType = bridgeTypeToSynapseType.get(bridgeType);
            if (synapseType == null) {
                System.out.println("No Synapse type found for Bridge type " + bridgeType);
                synapseType = ColumnType.STRING;
            }

            ColumnModel oneColumn = new ColumnModel();
            oneColumn.setName(name);
            oneColumn.setColumnType(synapseType);

            if (synapseType == ColumnType.STRING) {
                Integer maxLength = bridgeTypeToMaxLength.get(bridgeType);
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
        // TODO: ACLs
        TableEntity createdTable = synapseClient.createEntity(synapseTable);
        synapseTableId = createdTable.getId();

        // write back to DDB table
        Item synapseTableDdbItem = new Item();
        synapseTableDdbItem.withString("schemaKey", schemaKey.toString());
        synapseTableDdbItem.withString("tableId", synapseTableId);
        synapseTablesTable.putItem(synapseTableDdbItem);

        // write back to cache
        synapseTableIdMap.put(schemaKey, synapseTableId);

        return synapseTableId;
    }

    public String serializeToSynapseType(String recordId, String bridgeType, JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        ColumnType synapseType = bridgeTypeToSynapseType.get(bridgeType);
        if (synapseType == null) {
            System.out.println("No Synapse type found for Bridge type " + bridgeType + ", record ID " + recordId);
            synapseType = ColumnType.STRING;
        }

        switch (synapseType) {
            case BOOLEAN:
                if (node.isBoolean()) {
                    return String.valueOf(node.booleanValue());
                }
                return null;
            case DATE:
                // date is a long epoch millis
                if (node.isTextual()) {
                    try {
                        DateTime dateTime = DateTime.parse(node.textValue());
                        return String.valueOf(dateTime.getMillis());
                    } catch (RuntimeException ex) {
                        // throw out malformatted dates
                        return null;
                    }
                } else if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case DOUBLE:
                // double only has double precision, not BigDecimal precision
                if (node.isNumber()) {
                    return String.valueOf(node.doubleValue());
                }
                return null;
            case FILEHANDLEID:
                // TODO
                return null;
            case INTEGER:
                // int includes long
                if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case STRING:
                // String includes calendar_date (as JSON string) and inline_json_blob (arbitrary JSON)
                String nodeValue;
                if ("inline_json_blob".equalsIgnoreCase(bridgeType)) {
                    nodeValue = node.toString();
                } else {
                    nodeValue = node.textValue();
                }

                Integer maxLength = bridgeTypeToMaxLength.get(bridgeType);
                if (maxLength == null) {
                    System.out.println("No max length found for Bridge type " + bridgeType);
                    maxLength = 1000;
                }
                return BridgeExporter.trimToLengthAndWarn(nodeValue, maxLength);
            default:
                System.out.println("Unexpected Synapse Type " + String.valueOf(synapseType) + " for record ID "
                        + recordId);
                return null;
        }
    }
}
