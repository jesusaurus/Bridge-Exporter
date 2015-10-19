package org.sagebionetworks.bridge.exporter.sharing;

import java.io.IOException;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.cache.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

// TODO doc
public class DynamoSharingScopeCacheLoader extends CacheLoader<String, SharingScope> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoSharingScopeCacheLoader.class);

    private Table ddbParticipantOptionsTable;

    public final void setDdbParticipantOptionsTable(Table ddbParticipantOptionsTable) {
        this.ddbParticipantOptionsTable = ddbParticipantOptionsTable;
    }

    @Override
    public SharingScope load(String healthCode) {
        // default sharing scope is no sharing
        SharingScope sharingScope = SharingScope.NO_SHARING;

        try {
            Item participantOptionsItem = ddbParticipantOptionsTable.getItem("healthDataCode", healthCode);
            if (participantOptionsItem != null) {
                String participantOptionsData = participantOptionsItem.getString("data");
                Map<String, Object> participantOptionsDataMap = DefaultObjectMapper.INSTANCE.readValue(
                        participantOptionsData, DefaultObjectMapper.TYPE_REF_RAW_MAP);
                String sharingScopeStr = String.valueOf(participantOptionsDataMap.get("SHARING_SCOPE"));

                // put this in its own try-catch block for better logging
                try {
                    sharingScope = SharingScope.valueOf(sharingScopeStr);
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to parse sharing options for hash[healthCode]=" + healthCode.hashCode() +
                            ", sharing scope value=" + sharingScopeStr);
                }
            }
        } catch (IOException | RuntimeException ex) {
            // log an error, fall back to default
            LOG.error("Unable to get sharing options for hash[healthCode]=" + healthCode.hashCode() + ": " +
                    ex.getMessage(), ex);
        }

        return sharingScope;
    }
}
