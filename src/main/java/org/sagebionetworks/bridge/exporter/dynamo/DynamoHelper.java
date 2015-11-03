package org.sagebionetworks.bridge.exporter.dynamo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.jcabi.aspects.Cacheable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/**
 * Encapsulates common calls Bridge-EX makes to DDB. Some of these have a little bit of logic or require marshalling
 * from a DDB item to a Bridge-EX object. Others are there just for convenience, so we have all or most of our DDB
 * interactions through a single class.
 */
@Component
public class DynamoHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoHelper.class);

    private Table ddbParticipantOptionsTable;
    private Table ddbSchemaTable;
    private Table ddbStudyTable;

    /** Participant options table, used to get user sharing scope. */
    @Resource(name = "ddbParticipantOptionsTable")
    public final void setDdbParticipantOptionsTable(Table ddbParticipantOptionsTable) {
        this.ddbParticipantOptionsTable = ddbParticipantOptionsTable;
    }

    /** Schema table, used to get schemas to create Synapse tables and serialize health data. */
    @Resource(name = "ddbSchemaTable")
    public final void setDdbSchemaTable(Table ddbSchemaTable) {
        this.ddbSchemaTable = ddbSchemaTable;
    }

    /** Study table, used to get study config, like linked Synapse project. */
    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    /**
     * Returns the schema for the given key.
     *
     * @param metrics
     *         metrics object, used to keep a record of "schemas not found"
     * @param schemaKey
     *         key for the schema to get
     * @return the schema
     * @throws IOException
     *         if we fail to deserialize the schema
     * @throws SchemaNotFoundException
     *         if the schema doesn't exist
     */
    public UploadSchema getSchema(Metrics metrics, UploadSchemaKey schemaKey) throws IOException,
            SchemaNotFoundException {
        Item schemaItem = getSchemaCached(schemaKey);
        if (schemaItem == null) {
            metrics.addKeyValuePair("schemasNotFound", schemaKey.toString());
            throw new SchemaNotFoundException("Schema not found: " + schemaKey.toString());
        }
        return UploadSchema.fromDdbItem(schemaItem);
    }

    // Helper method that encapsulates just the DDB call, cached with annotation.
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    private Item getSchemaCached(UploadSchemaKey schemaKey) {
        return ddbSchemaTable.getItem("key", schemaKey.getStudyId() + ":" + schemaKey.getSchemaId(),
                "revision", schemaKey.getRevision());
    }

    /**
     * Helper method to get the schema key for a DDB health data record
     *
     * @param record
     *         DDB Item representing a health data record
     * @return the schema key associated with that record
     */
    public static UploadSchemaKey getSchemaKeyForRecord(Item record) {
        String studyId = record.getString("studyId");
        String schemaId = record.getString("schemaId");
        int schemaRev = record.getInt("schemaRevision");
        return new UploadSchemaKey.Builder().withStudyId(studyId).withSchemaId(schemaId).withRevision(schemaRev)
                .build();
    }

    /**
     * Gets the sharing scope for the given user.
     *
     * @param healthCode
     *         health code of user to get sharing scope for
     * @return the user's sharing scope
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public SharingScope getSharingScopeForUser(String healthCode) {
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

    /**
     * Get study info, namely Synapse project and data access team.
     *
     * @param studyId
     *         study ID to fetch
     * @return study info
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public StudyInfo getStudyInfo(String studyId) {
        Item studyItem = ddbStudyTable.getItem("identifier", studyId);
        return new StudyInfo.Builder().withDataAccessTeamId(studyItem.getLong("synapseDataAccessTeamId"))
                .withSynapseProjectId(studyItem.getString("synapseProjectId")).build();
    }
}
