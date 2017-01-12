package org.sagebionetworks.bridge.exporter.synapse;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Joiner;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Enum class representing several different transfer method from record value in ddb table to synapse row value.
 * Also specifying corresponding transfer procedure.
 */
public enum TransferMethod {
    STRING {
        @Override
        public String transfer(String ddbName, Item record, ExportTask task) {
            return record.getString(ddbName);
        }
    },
    STRINGSET {
        private final Joiner STRING_SET_JOINER = Joiner.on(',').useForNull("");

        @Override
        public String transfer(String ddbName, Item record, ExportTask task) {
            String valueToAdd = "";
            Set<String> stringSet = record.getStringSet(ddbName);
            if (stringSet != null) {
                List<String> stringSetList = new ArrayList<>();
                stringSetList.addAll(stringSet);
                Collections.sort(stringSetList);
                valueToAdd = STRING_SET_JOINER.join(stringSetList);
            }
            return valueToAdd;
        }
    },
    DATE {
        @Override
        public String transfer(String ddbName, Item record, ExportTask task) {
            return String.valueOf(record.getLong(ddbName));
        }
    },
    EXPORTERDATE {
        @Override
        public String transfer(String ddbName, Item record, ExportTask task) {
            return task.getExporterDate().toString();
        }
    };

    public abstract String transfer(final String ddbName, final Item record, final ExportTask task);
}
