package org.sagebionetworks.bridge.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/** Synapse export worker for app version tables. */
public class AppVersionExportWorker extends SynapseExportWorker {
    @Override
    protected String getDdbTableName() {
        return "SynapseMetaTables";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "tableName";
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();

        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnList.add(recordIdColumn);

        ColumnModel healthCodeColumn = new ColumnModel();
        healthCodeColumn.setName("healthCode");
        healthCodeColumn.setColumnType(ColumnType.STRING);
        healthCodeColumn.setMaximumSize(36L);
        columnList.add(healthCodeColumn);

        ColumnModel externalIdColumn = new ColumnModel();
        externalIdColumn.setName("externalId");
        externalIdColumn.setColumnType(ColumnType.STRING);
        externalIdColumn.setMaximumSize(128L);
        columnList.add(externalIdColumn);

        ColumnModel originalTableColumn = new ColumnModel();
        originalTableColumn.setName("originalTable");
        originalTableColumn.setColumnType(ColumnType.STRING);
        originalTableColumn.setMaximumSize(128L);
        columnList.add(originalTableColumn);

        ColumnModel appVersionColumn = new ColumnModel();
        appVersionColumn.setName("appVersion");
        appVersionColumn.setColumnType(ColumnType.STRING);
        appVersionColumn.setMaximumSize(48L);
        columnList.add(appVersionColumn);

        ColumnModel phoneInfoColumn = new ColumnModel();
        phoneInfoColumn.setName("phoneInfo");
        phoneInfoColumn.setColumnType(ColumnType.STRING);
        phoneInfoColumn.setMaximumSize(48L);
        columnList.add(phoneInfoColumn);

        return columnList;
    }

    @Override
    protected String getSynapseTableName() {
        return getStudyId() + "-appVersion";
    }

    @Override
    protected List<String> getTsvHeaderList() {
        List<String> fieldNameList = new ArrayList<>();
        fieldNameList.add("recordId");
        fieldNameList.add("healthCode");
        fieldNameList.add("externalId");
        fieldNameList.add("originalTable");
        fieldNameList.add("appVersion");
        fieldNameList.add("phoneInfo");
        return fieldNameList;
    }

    @Override
    protected List<String> getTsvRowValueList(ExportTask task) throws ExportWorkerException {
        Item record = task.getRecord();
        if (record == null) {
            throw new ExportWorkerException("Null record for AppVersionExportWorker");
        }
        String recordId = record.getString("id");

        // construct original schema
        String studyId = record.getString("studyId");
        String schemaId = record.getString("schemaId");
        int schemaRev = record.getInt("schemaRevision");
        UploadSchemaKey originalSchemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

        // get phone and app info
        String appVersion = null;
        String phoneInfo = null;
        String metadataString = record.getString("metadata");
        if (!Strings.isNullOrEmpty(metadataString)) {
            try {
                JsonNode metadataJson = BridgeExporterUtil.JSON_MAPPER.readTree(metadataString);
                appVersion = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "appVersion", 48,
                        recordId);
                phoneInfo = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "phoneInfo", 48, recordId);
            } catch (IOException ex) {
                // we can recover from this
                System.out.println("Error parsing metadata for record ID " + recordId + ": " + ex.getMessage());
            }
        }

        // construct row
        List<String> rowValueList = new ArrayList<>();
        rowValueList.add(recordId);
        rowValueList.add(record.getString("healthCode"));
        rowValueList.add(BridgeExporterUtil.getDdbStringRemoveTabsAndTrim(record, "userExternalId", 128, recordId));
        rowValueList.add(originalSchemaKey.toString());
        rowValueList.add(appVersion);
        rowValueList.add(phoneInfo);

        return rowValueList;
    }
}
