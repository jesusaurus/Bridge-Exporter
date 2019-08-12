package org.sagebionetworks.bridge.exporter.synapse;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.repo.model.table.ColumnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enum class representing several different transfer method from record value in ddb table to synapse row value.
 * Also specifying corresponding transfer procedure.
 */
public enum TransferMethod {
    INT {
        @Override
        public String transfer(String ddbName, Item record) {
            return String.valueOf(record.getInt(ddbName));
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.INTEGER;
        }
    },
    STRING {
        @Override
        public String transfer(String ddbName, Item record) {
            return record.getString(ddbName);
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.STRING;
        }
    },
    STRINGSET {
        @Override
        public String transfer(String ddbName, Item record) {
            String valueToAdd = "";
            Set<String> stringSet = record.getStringSet(ddbName);
            if (stringSet != null) {
                List<String> stringSetList = new ArrayList<>();
                stringSetList.addAll(stringSet);
                Collections.sort(stringSetList);
                valueToAdd = BridgeExporterUtil.STRING_SET_JOINER.join(stringSetList);
            }
            return valueToAdd;
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.STRING;
        }
    },
    STRINGMAP {
        @Override
        public String transfer(String ddbName, Item record) {
            Map<String,String> substudyMap = record.getMap(ddbName);
            if (substudyMap == null || substudyMap.isEmpty()) {
                return null;
            }
            return BridgeExporterUtil.serializeSubstudyMemberships(substudyMap);
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.STRING;
        }
    },
    DATE {
        @Override
        public String transfer(String ddbName, Item record) {
            return String.valueOf(record.getLong(ddbName));
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.DATE;
        }
    },
    LARGETEXT {
        @Override
        public String transfer(String ddbName, Item record) {
            return record.getString(ddbName);
        }

        @Override
        public ColumnType getColumnType() {
            return ColumnType.LARGETEXT;
        }
    };

    public abstract String transfer(final String ddbName, final Item record);

    // helper method to get column type from transfer method -- since they share same value in exporter
    public abstract ColumnType getColumnType();
}
