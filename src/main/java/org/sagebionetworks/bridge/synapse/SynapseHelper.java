package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

import org.sagebionetworks.bridge.exporter.BridgeExporter;
import org.sagebionetworks.bridge.s3.S3Helper;

public class SynapseHelper {
    private static final Set<ACCESS_TYPE> ACCESS_TYPE_ALL = ImmutableSet.copyOf(ACCESS_TYPE.values());
    private static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final long SYNAPSE_APPEND_ROW_TIMEOUT_MILLISECONDS = 5000L;   // 5 seconds

    public static final Map<String, ColumnType> BRIDGE_TYPE_TO_SYNAPSE_TYPE =
            ImmutableMap.<String, ColumnType>builder()
                    .put("attachment_blob", ColumnType.FILEHANDLEID)
                    .put("attachment_csv", ColumnType.FILEHANDLEID)
                    .put("attachment_json_blob", ColumnType.FILEHANDLEID)
                    .put("attachment_json_table", ColumnType.FILEHANDLEID)
                    .put("boolean", ColumnType.BOOLEAN)
                    .put("calendar_date", ColumnType.STRING)
                    .put("float", ColumnType.DOUBLE)
                    .put("inline_json_blob", ColumnType.STRING)
                    .put("int", ColumnType.INTEGER)
                    .put("string", ColumnType.STRING)
                    .put("timestamp", ColumnType.DATE)
                    .build();

    public static final Map<String, Integer> BRIDGE_TYPE_TO_MAX_LENGTH = ImmutableMap.<String, Integer>builder()
            .put("calendar_date", 10)
            .put("inline_json_blob", 1000)
            .put("string", 1000)
            .build();

    private final Map<String, List<SelectColumn>> synapseAppendRowHeaderMap = new HashMap<>();
    private final Map<String, String> synapseMetaTableMap = new HashMap<>();

    private Map<String, Long> dataAccessTeamIdsByStudy;
    private DynamoDB ddbClient;
    private long principalId;
    private Map<String, String> projectIdsByStudy;
    private S3Helper s3Helper;
    private SynapseClient synapseClient;
    private Table synapseMetaTablesTable;

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public void init() throws IOException, SynapseException {
        // ddb tables
        synapseMetaTablesTable = ddbClient.getTable("prod-exporter-SynapseMetaTables");

        // synapse config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        JsonNode synapseConfigJson = JSON_MAPPER.readTree(synapseConfigFile);
        String synapseUserName = synapseConfigJson.get("username").textValue();
        String synapseApiKey = synapseConfigJson.get("apiKey").textValue();
        String synapsePassword = synapseConfigJson.get("password").textValue();
        principalId = synapseConfigJson.get("principalId").longValue();

        // synapse client
        synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(synapseUserName);
        synapseClient.setApiKey(synapseApiKey);

        // because of a bug in the Java client, we need to properly log in to upload file handles
        // see https://sagebionetworks.jira.com/browse/PLFM-3310
        synapseClient.login(synapseUserName, synapsePassword);

        // project ID mapping
        projectIdsByStudy = new HashMap<>();
        JsonNode projectIdMapNode = synapseConfigJson.get("projectIdsByStudy");
        Iterator<String> projectIdMapKeyIter = projectIdMapNode.fieldNames();
        while (projectIdMapKeyIter.hasNext()) {
            String oneStudyId = projectIdMapKeyIter.next();
            projectIdsByStudy.put(oneStudyId, projectIdMapNode.get(oneStudyId).textValue());
        }

        // data access team ID mapping
        dataAccessTeamIdsByStudy = new HashMap<>();
        JsonNode dataAccessTeamIdMapNode = synapseConfigJson.get("dataAccessTeamIdsByStudy");
        Iterator<String> dataAccessTeamIdMapKeyIter = dataAccessTeamIdMapNode.fieldNames();
        while (dataAccessTeamIdMapKeyIter.hasNext()) {
            String oneStudyId = dataAccessTeamIdMapKeyIter.next();
            dataAccessTeamIdsByStudy.put(oneStudyId, dataAccessTeamIdMapNode.get(oneStudyId).longValue());
        }
    }

    public void close() {
        if (synapseClient != null) {
            try {
                synapseClient.logout();
            } catch (SynapseException ex) {
                ex.printStackTrace();
            }
        }
    }

    public String getSynapseAppVersionTableForStudy(String studyId) throws SynapseException {
        String tableName = studyId + "-appVersion";

        // check cache
        String synapseTableId = synapseMetaTableMap.get(tableName);
        if (synapseTableId != null) {
            return synapseTableId;
        }

        // check DDB table
        Iterable<Item> tableIterable = synapseMetaTablesTable.query("tableName", tableName);
        Iterator<Item> tableIterator = tableIterable.iterator();
        if (tableIterator.hasNext()) {
            Item oneTable = tableIterator.next();
            synapseTableId = oneTable.getString("tableId");
            synapseMetaTableMap.put(tableName, synapseTableId);
            return synapseTableId;
        }

        // create columns
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

        // create columns
        List<ColumnModel> createdColumnList = synapseClient.createColumnModels(columnList);
        if (columnList.size() != createdColumnList.size()) {
            throw new RuntimeException("Tried to create " + columnList.size() + " columns. Actual: "
                    + createdColumnList.size() + " columns.");
        }

        List<String> columnIdList = new ArrayList<>();
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            columnIdList.add(oneCreatedColumn.getId());
        }

        // create table
        TableEntity synapseTable = new TableEntity();
        synapseTable.setName(tableName);
        synapseTable.setParentId(projectIdsByStudy.get(studyId));
        synapseTable.setColumnIds(columnIdList);
        TableEntity createdTable = synapseClient.createEntity(synapseTable);
        synapseTableId = createdTable.getId();

        createAclsForTableInStudy(studyId, synapseTableId);

        // write back to DDB table
        Item synapseTableDdbItem = new Item();
        synapseTableDdbItem.withString("tableName", tableName);
        synapseTableDdbItem.withString("tableId", synapseTableId);
        synapseMetaTablesTable.putItem(synapseTableDdbItem);

        // write back to cache
        synapseMetaTableMap.put(tableName, synapseTableId);

        return synapseTableId;
    }

    public void createAclsForTableInStudy(String studyId, String tableId) throws SynapseException {
        Set<ResourceAccess> resourceAccessSet = new HashSet<>();

        ResourceAccess exporterOwnerAccess = new ResourceAccess();
        exporterOwnerAccess.setPrincipalId(principalId);
        exporterOwnerAccess.setAccessType(ACCESS_TYPE_ALL);
        resourceAccessSet.add(exporterOwnerAccess);

        ResourceAccess dataAccessTeamAccess = new ResourceAccess();
        dataAccessTeamAccess.setPrincipalId(dataAccessTeamIdsByStudy.get(studyId));
        dataAccessTeamAccess.setAccessType(ACCESS_TYPE_READ);
        resourceAccessSet.add(dataAccessTeamAccess);

        AccessControlList acl = new AccessControlList();
        acl.setId(tableId);
        acl.setResourceAccess(resourceAccessSet);

        synapseClient.createACL(acl);
    }

    public List<SelectColumn> getSynapseAppendRowHeaders(String tableId) throws SynapseException {
        // check cache
        List<SelectColumn> headerList = synapseAppendRowHeaderMap.get(tableId);
        if (headerList != null) {
            return headerList;
        }

        // call Synapse
        List<ColumnModel> columnList = synapseClient.getColumnModelsForTableEntity(tableId);
        headerList = new ArrayList<>();
        for (ColumnModel oneColumn : columnList) {
            SelectColumn oneHeader = new SelectColumn();
            oneHeader.setId(oneColumn.getId());
            oneHeader.setName(oneColumn.getName());
            headerList.add(oneHeader);
        }

        // write back to cache
        synapseAppendRowHeaderMap.put(tableId, headerList);

        return headerList;
    }

    public void appendSynapseRows(RowSet rowSet, String tableId) throws SynapseException, InterruptedException {
        synapseClient.appendRowsToTable(rowSet, SYNAPSE_APPEND_ROW_TIMEOUT_MILLISECONDS, tableId);
    }

    public String serializeToSynapseType(String studyId, String recordId, String fieldName, String bridgeType,
            JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        ColumnType synapseType = BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
        if (synapseType == null) {
            System.out.println("No Synapse type found for Bridge type " + bridgeType + ", record ID " + recordId);
            synapseType = ColumnType.STRING;
        }

        switch (synapseType) {
            case BOOLEAN:
                if (node.isBoolean()) {
                    return String.valueOf(node.booleanValue());
                }
                return null;
            case DATE:
                // date is a long epoch millis
                if (node.isTextual()) {
                    try {
                        DateTime dateTime = DateTime.parse(node.textValue());
                        return String.valueOf(dateTime.getMillis());
                    } catch (RuntimeException ex) {
                        // throw out malformatted dates
                        return null;
                    }
                } else if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case DOUBLE:
                // double only has double precision, not BigDecimal precision
                if (node.isNumber()) {
                    return String.valueOf(node.doubleValue());
                }
                return null;
            case FILEHANDLEID:
                // file handles are text nodes, where the text is the attachment ID (which is the S3 Key)
                if (node.isTextual()) {
                    String s3Key = node.textValue();
                    try {
                        return uploadFromS3ToSynapseFileHandle(studyId, fieldName, s3Key);
                    } catch (IOException | SynapseException ex) {
                        System.out.println("Error uploading attachment to Synapse for record ID " + recordId +
                                ", s3Key " + s3Key + ": " + ex.getMessage());
                        return null;
                    }
                }
                return null;
            case INTEGER:
                // int includes long
                if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case STRING:
                // String includes calendar_date (as JSON string) and inline_json_blob (arbitrary JSON)
                String nodeValue;
                if ("inline_json_blob".equalsIgnoreCase(bridgeType)) {
                    nodeValue = node.toString();
                } else {
                    nodeValue = node.textValue();
                }

                Integer maxLength = BRIDGE_TYPE_TO_MAX_LENGTH.get(bridgeType);
                if (maxLength == null) {
                    System.out.println("No max length found for Bridge type " + bridgeType);
                    maxLength = 1000;
                }
                return BridgeExporter.trimToLengthAndWarn(nodeValue, maxLength);
            default:
                System.out.println("Unexpected Synapse Type " + String.valueOf(synapseType) + " for record ID "
                        + recordId);
                return null;
        }
    }

    private String uploadFromS3ToSynapseFileHandle(String studyId, String fieldName, String s3Key) throws IOException, SynapseException {
        // create temp file using the field name and s3Key as the prefix and the default suffix (null)
        File tempFile = File.createTempFile(fieldName + "-" + s3Key, null);

        try {
            // download from S3
            // TODO: update S3Helper to stream directly to a file instead of holding it in memory first
            byte[] fileBytes = s3Helper.readS3FileAsBytes(BridgeExporter.S3_BUCKET_ATTACHMENTS, s3Key);
            Files.write(fileBytes, tempFile);

            // upload to Synapse
            FileHandle synapseFileHandle = synapseClient.createFileHandle(tempFile, "application/octet-stream",
                    projectIdsByStudy.get(studyId));
            return synapseFileHandle.getId();
        } finally {
            // delete temp file
            tempFile.delete();
        }
    }
}
