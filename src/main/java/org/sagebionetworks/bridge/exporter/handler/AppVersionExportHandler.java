package org.sagebionetworks.bridge.exporter.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;

/**
 * Synapse export worker for app version tables. The app version is a master table keeping track of all records by
 * app version, upload date, and original table, along with any other relevant attributes.
 */
public class AppVersionExportHandler extends SynapseExportHandler {
    private final static List<ColumnModel> APPVERSION_COLUMN_LIST;
    static {
        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel originalTableColumn = new ColumnModel();
        originalTableColumn.setName("originalTable");
        originalTableColumn.setColumnType(ColumnType.STRING);
        originalTableColumn.setMaximumSize(128L);
        columnListBuilder.add(originalTableColumn);

        APPVERSION_COLUMN_LIST = columnListBuilder.build();
    }

    @Override
    protected String getDdbTableName() {
        return "SynapseMetaTables";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "tableName";
    }

    @Override
    protected String getDdbTableKeyValue() {
        return getStudyId() + "-appVersion";
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList() {
        return APPVERSION_COLUMN_LIST;
    }

    @Override
    protected TsvInfo getTsvInfoForTask(ExportTask task) {
        return task.getAppVersionTsvInfoForStudy(getStudyId());
    }

    @Override
    protected void setTsvInfoForTask(ExportTask task, TsvInfo tsvInfo) {
        task.setAppVersionTsvInfoForStudy(getStudyId(), tsvInfo);
    }

    @Override
    protected Map<String, String> getTsvRowValueMap(ExportSubtask subtask) {
        ExportTask task = subtask.getParentTask();
        Item record = subtask.getOriginalRecord();

        // book keeping - unique app versions
        PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(record);
        String appVersion = phoneAppVersionInfo.getAppVersion();
        if (StringUtils.isNotBlank(appVersion)) {
            task.getMetrics().addKeyValuePair("uniqueAppVersions[" + getStudyId() + "]", appVersion);
        }

        // construct row
        Map<String, String> rowValueMap = new HashMap<>();
        rowValueMap.put("originalTable", subtask.getSchemaKey().toString());
        return rowValueMap;
    }
}
