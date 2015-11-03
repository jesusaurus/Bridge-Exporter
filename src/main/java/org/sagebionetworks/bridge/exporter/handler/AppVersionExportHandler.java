package org.sagebionetworks.bridge.exporter.handler;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.TsvInfo;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

/**
 * Synapse export worker for app version tables. The app version is a master table keeping track of all records by
 * app version, upload date, and original table, along with any other relevant attributes.
 */
public class AppVersionExportHandler extends SynapseExportHandler {
    private final static List<ColumnModel> SYNAPSE_COLUMN_LIST;
    static {
        ImmutableList.Builder<ColumnModel> columnListBuilder = ImmutableList.builder();

        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnListBuilder.add(recordIdColumn);

        ColumnModel healthCodeColumn = new ColumnModel();
        healthCodeColumn.setName("healthCode");
        healthCodeColumn.setColumnType(ColumnType.STRING);
        healthCodeColumn.setMaximumSize(36L);
        columnListBuilder.add(healthCodeColumn);

        ColumnModel externalIdColumn = new ColumnModel();
        externalIdColumn.setName("externalId");
        externalIdColumn.setColumnType(ColumnType.STRING);
        externalIdColumn.setMaximumSize(128L);
        columnListBuilder.add(externalIdColumn);

        // NOTE: ColumnType.DATE is actually a timestamp. There is no calendar date type.
        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName("uploadDate");
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);
        columnListBuilder.add(uploadDateColumn);

        ColumnModel originalTableColumn = new ColumnModel();
        originalTableColumn.setName("originalTable");
        originalTableColumn.setColumnType(ColumnType.STRING);
        originalTableColumn.setMaximumSize(128L);
        columnListBuilder.add(originalTableColumn);

        ColumnModel appVersionColumn = new ColumnModel();
        appVersionColumn.setName("appVersion");
        appVersionColumn.setColumnType(ColumnType.STRING);
        appVersionColumn.setMaximumSize(48L);
        columnListBuilder.add(appVersionColumn);

        ColumnModel phoneInfoColumn = new ColumnModel();
        phoneInfoColumn.setName("phoneInfo");
        phoneInfoColumn.setColumnType(ColumnType.STRING);
        phoneInfoColumn.setMaximumSize(48L);
        columnListBuilder.add(phoneInfoColumn);

        SYNAPSE_COLUMN_LIST = columnListBuilder.build();
    }

    private static final List<String> TSV_HEADER_LIST = ImmutableList.of("recordId", "healthCode", "externalId",
            "uploadDate", "originalTable", "appVersion", "phoneInfo");

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
        return SYNAPSE_COLUMN_LIST;
    }

    @Override
    protected List<String> getTsvHeaderList() {
        return TSV_HEADER_LIST;
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
    protected List<String> getTsvRowValueList(ExportSubtask subtask) {
        ExportTask task = subtask.getParentTask();
        Item record = subtask.getOriginalRecord();
        String recordId = record.getString("id");

        // get phone and app info
        PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(record);
        String appVersion = phoneAppVersionInfo.getAppVersion();
        String phoneInfo = phoneAppVersionInfo.getPhoneInfo();

        // construct row
        List<String> rowValueList = new ArrayList<>();
        rowValueList.add(recordId);
        rowValueList.add(record.getString("healthCode"));
        rowValueList.add(BridgeExporterUtil.sanitizeDdbValue(record, "userExternalId", 128, recordId));
        rowValueList.add(task.getExporterDate().toString());
        rowValueList.add(subtask.getSchemaKey().toString());
        rowValueList.add(appVersion);
        rowValueList.add(phoneInfo);

        // book keeping
        if (StringUtils.isNotBlank(appVersion)) {
            task.getMetrics().addKeyValuePair("uniqueAppVersions[" + getStudyId() + "]", appVersion);
        }

        return rowValueList;
    }
}
