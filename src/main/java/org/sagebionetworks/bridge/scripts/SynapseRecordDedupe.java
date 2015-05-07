package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSelection;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseTableIterator;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class SynapseRecordDedupe {
    private static final Joiner COLUMN_JOINER = Joiner.on(", ").useForNull("null");

    public static void main(String[] args) throws Exception {
        // args
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: SynapseRecordDedupe [synapseTableId]");
        }
        String synapseTableId = args[0];

        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // create iterator
        SynapseTableIterator tableIter = new SynapseTableIterator(synapseClient,
                "SELECT recordId FROM " + synapseTableId, synapseTableId);

        // Iterate over table rows. Use a ListMultimap instead of a SetMultimap, so we can choose to keep the "first"
        // rowId.
        ListMultimap<String, Long> recordIdToRowId = ArrayListMultimap.create();
        while (tableIter.hasNext()) {
            Row row = tableIter.next();
            Long rowId = row.getRowId();

            // We only select recordId, so values should have size 1, and the only entry is the record ID.
            String recordId = row.getValues().get(0);
            recordIdToRowId.put(recordId, rowId);
        }

        // find duplicate record IDs
        List<Long> rowIdsToDelete = new ArrayList<>();
        for (String oneRecordId : recordIdToRowId.keySet()) {
            List<Long> recordRowIdList = recordIdToRowId.get(oneRecordId);
            if (recordRowIdList.size() > 1) {
                rowIdsToDelete.addAll(recordRowIdList.subList(1, recordRowIdList.size()));
            }
        }

        // delete rows from Synapse table
        RowSelection rowSelectionToDelete = new RowSelection();
        rowSelectionToDelete.setEtag(tableIter.getEtag());
        rowSelectionToDelete.setRowIds(rowIdsToDelete);
        rowSelectionToDelete.setTableId(synapseTableId);
        synapseClient.deleteRowsFromTable(rowSelectionToDelete);

        System.out.println("Deleted " + rowIdsToDelete.size() + " rows");
    }
}
