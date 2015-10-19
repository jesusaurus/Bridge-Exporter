package org.sagebionetworks.bridge.exporter.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

// TODO doc
public class DynamoHelper {
    private Table ddbRecordTable;

    public final void setDdbRecordTable(Table ddbRecordTable) {
        this.ddbRecordTable = ddbRecordTable;
    }

    public Item getRecord(String recordId) {
        return ddbRecordTable.getItem("id", recordId);
    }
}
