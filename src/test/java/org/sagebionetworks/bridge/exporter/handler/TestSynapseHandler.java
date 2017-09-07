package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;

// Used for testing SynapseExportHandler. See SynapseExportHandlerTest and SynapseExportHandlerNewTableTest
public class TestSynapseHandler extends SynapseExportHandler {
    private TsvInfo tsvInfo;

    @Override
    protected String getDdbTableName() {
        return "TestDdbTable";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "testId";
    }

    @Override
    protected String getDdbTableKeyValue() {
        return "foobarbaz";
    }

    @Override
    protected String getSynapseTableName() {
        return "Test Synapse Table Name";
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList(ExportTask task) {
        List<ColumnModel> columnList = new ArrayList<>();

        ColumnModel fooColumn = new ColumnModel();
        fooColumn.setName("foo");
        fooColumn.setColumnType(ColumnType.INTEGER);
        columnList.add(fooColumn);
        return columnList;
    }

    // For test purposes only, we store it directly in the test handler.
    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return tsvInfo;
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        this.tsvInfo = tsvInfo;
    }

    // For test purposes, our test data will just be
    // {
    //   "foo":"value"
    // }
    //
    // We write that value to our string list (of size 1, because we have only 1 column).
    //
    // However, if we see the "error" key, throw an IOException with that error message. This is to test
    // error handling.
    @Override
    protected Map<String, String> getTsvRowValueMap(ExportSubtask subtask) throws IOException {
        JsonNode dataNode = subtask.getRecordData();
        if (dataNode.has("error")) {
            throw new IOException(dataNode.get("error").textValue());
        }

        String value = dataNode.get("foo").textValue();
        return ImmutableMap.of("foo", value);
    }
}
