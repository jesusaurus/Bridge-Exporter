package org.sagebionetworks.bridge.exporter.handler;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.MetaTableType;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;

/** This worker exports schemaless records into the default table. */
public class SchemalessExportHandler extends HealthDataExportHandler {
    @Override
    protected String getDdbTableName() {
        return SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES;
    }

    @Override
    protected String getDdbTableKeyName() {
        return SynapseHelper.DDB_KEY_TABLE_NAME;
    }

    @Override
    protected String getDdbTableKeyValue() {
        return getStudyId() + "-default";
    }

    @Override
    protected String getSynapseTableName() {
        return BridgeExporterUtil.DEFAULT_TABLE_NAME;
    }

    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return task.getTsvInfoForStudyAndType(getStudyId(), MetaTableType.DEFAULT);
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        task.setTsvInfoForStudyAndType(getStudyId(), MetaTableType.DEFAULT, tsvInfo);
    }

    @Override
    protected List<UploadFieldDefinition> getSchemaFieldDefList(Metrics metrics) {
        return ImmutableList.of();
    }
}
