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

// TODO doc
@Component
public class DynamoHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoHelper.class);

    private Table ddbParticipantOptionsTable;
    private Table ddbSchemaTable;
    private Table ddbStudyTable;

    @Resource(name = "ddbParticipantOptionsTable")
    public final void setDdbParticipantOptionsTable(Table ddbParticipantOptionsTable) {
        this.ddbParticipantOptionsTable = ddbParticipantOptionsTable;
    }

    @Resource(name = "ddbSchemaTable")
    public final void setDdbSchemaTable(Table ddbSchemaTable) {
        this.ddbSchemaTable = ddbSchemaTable;
    }

    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    public UploadSchema getSchema(Metrics metrics, UploadSchemaKey schemaKey) throws IOException,
            SchemaNotFoundException {
        Item schemaItem = getSchemaCached(schemaKey);
        if (schemaItem == null) {
            metrics.addKeyValuePair("schemasNotFound", schemaKey.toString());
            throw new SchemaNotFoundException("Schema not found: " + schemaKey.toString());
        }
        return UploadSchema.fromDdbItem(schemaItem);
    }

    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    private Item getSchemaCached(UploadSchemaKey schemaKey) {
        return ddbSchemaTable.getItem("key", schemaKey.getStudyId() + ":" + schemaKey.getSchemaId(),
                "revision", schemaKey.getRevision());
    }

    public static UploadSchemaKey getSchemaKeyForRecord(Item record) {
        String studyId = record.getString("studyId");
        String schemaId = record.getString("schemaId");
        int schemaRev = record.getInt("schemaRevision");
        return new UploadSchemaKey.Builder().withStudyId(studyId).withSchemaId(schemaId).withRevision(schemaRev)
                .build();
    }

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

    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public StudyInfo getStudyInfo(String studyId) {
        // TODO add these fields to the study table
        Item studyItem = ddbStudyTable.getItem("identifier", studyId);
        return new StudyInfo.Builder().withDataAccessTeamId(studyItem.getInt("synapseDataAccessTeamId"))
                .withSynapseProjectId("synapseProjectId").build();
    }
}
