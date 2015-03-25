package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

public class SynapseHelper {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final long SYNAPSE_APPEND_ROW_TIMEOUT_MILLISECONDS = 5000L;   // 5 seconds

    private final Map<String, List<SelectColumn>> synapseAppendRowHeaderMap = new HashMap<>();
    private final Map<String, String> synapseMetaTableMap = new HashMap<>();

    private DynamoDB ddbClient;
    private Map<String, String> projectIdsByStudy;
    private SynapseClient synapseClient;
    private Table synapseMetaTablesTable;

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    public void init() throws IOException {
        // ddb tables
        synapseMetaTablesTable = ddbClient.getTable("prod-exporter-SynapseMetaTables");

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

    public String getSynapseAppVersionTableForStudy(String studyId) throws SynapseException {
        String tableName = studyId + "-appVersion";

        // check cache
        String synapseTableId = synapseMetaTableMap.get(tableName);
        if (synapseTableId != null) {
            return synapseTableId;
        }

        // check DDB table
        Iterable<Item> tableIterable = synapseMetaTablesTable.query("tableName", tableName);
        Iterator<Item> tableIterator = tableIterable.iterator();
        if (tableIterator.hasNext()) {
            Item oneTable = tableIterator.next();
            synapseTableId = oneTable.getString("tableId");
            synapseMetaTableMap.put(tableName, synapseTableId);
            return synapseTableId;
        }

        // create columns
        List<ColumnModel> columnList = new ArrayList<>();

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

        ColumnModel originalTableColumn = new ColumnModel();
        originalTableColumn.setName("originalTable");
        originalTableColumn.setColumnType(ColumnType.STRING);
        originalTableColumn.setMaximumSize(128L);
        columnList.add(originalTableColumn);

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
        synapseTable.setName(tableName);
        synapseTable.setParentId(projectIdsByStudy.get(studyId));
        synapseTable.setColumnIds(columnIdList);
        // TODO: ACLs
        TableEntity createdTable = synapseClient.createEntity(synapseTable);
        synapseTableId = createdTable.getId();

        // write back to DDB table
        Item synapseTableDdbItem = new Item();
        synapseTableDdbItem.withString("tableName", tableName);
        synapseTableDdbItem.withString("tableId", synapseTableId);
        synapseMetaTablesTable.putItem(synapseTableDdbItem);

        // write back to cache
        synapseMetaTableMap.put(tableName, synapseTableId);

        return synapseTableId;
    }

    public List<SelectColumn> getSynapseAppendRowHeaders(String tableId) throws SynapseException {
        // check cache
        List<SelectColumn> headerList = synapseAppendRowHeaderMap.get(tableId);
        if (headerList != null) {
            return headerList;
        }

        // call Synapse
        List<ColumnModel> columnList = synapseClient.getColumnModelsForTableEntity(tableId);
        headerList = new ArrayList<>();
        for (ColumnModel oneColumn : columnList) {
            SelectColumn oneHeader = new SelectColumn();
            oneHeader.setId(oneColumn.getId());
            oneHeader.setName(oneColumn.getName());
            headerList.add(oneHeader);
        }

        // write back to cache
        synapseAppendRowHeaderMap.put(tableId, headerList);

        return headerList;
    }

    public void appendSynapseRows(RowSet rowSet, String tableId) throws SynapseException, InterruptedException {
        synapseClient.appendRowsToTable(rowSet, SYNAPSE_APPEND_ROW_TIMEOUT_MILLISECONDS, tableId);
    }
}
