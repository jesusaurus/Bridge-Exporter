package org.sagebionetworks.bridge.exporter.schema;

import com.amazonaws.services.dynamodbv2.document.Item;

import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// TODO
public class UploadSchemaHelper {
    public static UploadSchemaKey getSchemaKeyForRecord(Item record) {
        String studyId = record.getString("studyId");
        String schemaId = record.getString("schemaId");
        int schemaRev = record.getInt("schemaRevision");
        return new UploadSchemaKey.Builder().withStudyId(studyId).withSchemaId(schemaId).withRevision(schemaRev)
                .build();
    }
}
