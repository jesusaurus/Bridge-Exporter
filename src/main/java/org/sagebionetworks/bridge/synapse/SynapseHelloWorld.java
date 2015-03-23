package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

public class SynapseHelloWorld {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws SynapseException, InterruptedException, IOException {
        // get synapse credentials
        // TODO generalize this instead of getting it from home directory
        // TODO get this per study?
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse.json");
        JsonNode synapseConfigJson = JSON_OBJECT_MAPPER.readTree(synapseConfigFile);
        String synapseUserName = synapseConfigJson.get("username").textValue();
        String synapseApiKey = synapseConfigJson.get("apiKey").textValue();

        // synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(synapseUserName);
        synapseClient.setApiKey(synapseApiKey);

        // create columns foo and bar
        ColumnModel fooColumn = new ColumnModel();
        fooColumn.setName("foo");
        fooColumn.setColumnType(ColumnType.STRING);

        ColumnModel barColumn = new ColumnModel();
        barColumn.setName("bar");
        barColumn.setColumnType(ColumnType.INTEGER);

        ColumnModel bazColumn = new ColumnModel();
        bazColumn.setName("baz");
        bazColumn.setColumnType(ColumnType.BOOLEAN);

        List<ColumnModel> createdColumnList = synapseClient.createColumnModels(ImmutableList.of(fooColumn, barColumn,
                bazColumn));
        List<String> columnIdList = new ArrayList<>();
        for (ColumnModel oneColumn : createdColumnList) {
            columnIdList.add(oneColumn.getId());
        }

        // create table
        TableEntity table = new TableEntity();
        table.setName("DJeng test table");
        table.setColumnIds(columnIdList);
        table.setParentId("syn2915929");
        TableEntity createdTable = synapseClient.createEntity(table);
        String tableId = createdTable.getId();
        System.out.println("Table ID " + tableId);

        // create row
        Row row = new Row();
        row.setValues(ImmutableList.of("foo", "42", "false"));

        List<SelectColumn> headerList = new ArrayList<>();
        for (String oneColumnId : columnIdList) {
            SelectColumn oneHeader = new SelectColumn();
            oneHeader.setId(oneColumnId);
            headerList.add(oneHeader);
        }
        System.out.println("headers.size(): " + headerList.size());

        RowSet rowSet = new RowSet();
        rowSet.setHeaders(headerList);
        rowSet.setRows(Collections.singletonList(row));
        rowSet.setTableId(tableId);

        RowReferenceSet appendedRowSet = synapseClient.appendRowsToTable(rowSet, 30 * 1000, tableId);
        System.out.println("Wrote " + appendedRowSet.getRows().size() + " rows");
    }
}
