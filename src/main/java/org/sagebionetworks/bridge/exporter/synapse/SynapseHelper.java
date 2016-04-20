package org.sagebionetworks.bridge.exporter.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.jcabi.aspects.RetryOnFailure;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.config.SpringConfig;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.schema.UploadFieldTypes;

/** Helper class for Synapse calls, including complex logic around asynchronous calls and retry helper. */
@Component
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    private static final long APPEND_TIMEOUT_MILLISECONDS = 30 * 1000;

    // Config keys. Package-scoped to allow unit tests to mock.
    static final String CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS = "synapse.async.interval.millis";
    static final String CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS = "synapse.async.timeout.loops";

    // Shared constants.
    public static final Set<ACCESS_TYPE> ACCESS_TYPE_ALL = ImmutableSet.copyOf(ACCESS_TYPE.values());
    public static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);
    public static final String DDB_TABLE_SYNAPSE_META_TABLES = "SynapseMetaTables";
    public static final String DDB_KEY_TABLE_NAME = "tableName";

    /** Mapping from Bridge types to their respective max lengths. Only covers things that are strings in Synapse. */
    public static final Map<String, Integer> BRIDGE_TYPE_TO_MAX_LENGTH = ImmutableMap.<String, Integer>builder()
            .put(UploadFieldTypes.CALENDAR_DATE, 10)
            .put(UploadFieldTypes.INLINE_JSON_BLOB, 100)
            .put(UploadFieldTypes.STRING, 100)
            .build();

    /** Default max length for string columns in Synapse, if the mapping is absent from BRIDGE_TYPE_TO_MAX_LENGTH. */
    public static final int DEFAULT_MAX_LENGTH = 100;

    /** Mapping from attachment types to MIME types, for use with telling Synapse what kind of file handle this is. */
    public static final Map<String, String> BRIDGE_TYPE_TO_MIME_TYPE = ImmutableMap.<String, String>builder()
            .put(UploadFieldTypes.ATTACHMENT_BLOB, "application/octet-stream")
            .put(UploadFieldTypes.ATTACHMENT_CSV, "text/csv")
            .put(UploadFieldTypes.ATTACHMENT_JSON_BLOB, "text/json")
            .put(UploadFieldTypes.ATTACHMENT_JSON_TABLE, "text/json")
            .build();

    /** Mapping from Bridge schema column types to Synapse table column types. */
    public static final Map<String, ColumnType> BRIDGE_TYPE_TO_SYNAPSE_TYPE =
            ImmutableMap.<String, ColumnType>builder()
                    .put(UploadFieldTypes.ATTACHMENT_BLOB, ColumnType.FILEHANDLEID)
                    .put(UploadFieldTypes.ATTACHMENT_CSV, ColumnType.FILEHANDLEID)
                    .put(UploadFieldTypes.ATTACHMENT_JSON_BLOB, ColumnType.FILEHANDLEID)
                    .put(UploadFieldTypes.ATTACHMENT_JSON_TABLE, ColumnType.FILEHANDLEID)
                    .put(UploadFieldTypes.BOOLEAN, ColumnType.BOOLEAN)
                    .put(UploadFieldTypes.CALENDAR_DATE, ColumnType.STRING)
                    .put(UploadFieldTypes.FLOAT, ColumnType.DOUBLE)
                    .put(UploadFieldTypes.INLINE_JSON_BLOB, ColumnType.STRING)
                    .put(UploadFieldTypes.INT, ColumnType.INTEGER)
                    .put(UploadFieldTypes.STRING, ColumnType.STRING)
                    .put(UploadFieldTypes.TIMESTAMP, ColumnType.DATE)
                    .build();

    // config
    private int asyncIntervalMillis;
    private int asyncTimeoutLoops;
    private String attachmentBucket;

    // Spring helpers
    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private SynapseClient synapseClient;

    /** Config, used to get the attachment S3 bucket to get Bridge attachments. */
    @Autowired
    public final void setConfig(Config config) {
        this.asyncIntervalMillis = config.getInt(CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS);
        this.asyncTimeoutLoops = config.getInt(CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS);
        this.attachmentBucket = config.get(SpringConfig.CONFIG_KEY_ATTACHMENT_S3_BUCKET);
    }

    /** File helper, used when we need to create a temporary file for downloads and uploads. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** S3 Helper, used to download Bridge attachments before uploading them to Synapse. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Synapse client. */
    @Autowired
    public final void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    /**
     * Serializes a Bridge health data record column into a Synapse table column.
     *
     * @param tmpDir
     *         temp directory, used for scratch space for uploading attachments
     * @param projectId
     *         Synapse project ID, used to determine where to upload attachments to
     * @param recordId
     *         Bridge record ID, used for logging
     * @param fieldName
     *         record field name, used to determine attachment filenames
     * @param bridgeType
     *         bridge type to serialize from
     * @param node
     *         value to serialize
     * @return serialized value, to be uploaded to a Synapse table
     * @throws IOException
     *         if downloading the attachment from S3 fails
     * @throws SynapseException
     *         if uploading the attachment to Synapse fails
     */
    public String serializeToSynapseType(File tmpDir, String projectId, String recordId, String fieldName,
            String bridgeType, JsonNode node) throws IOException, SynapseException {
        if (node == null || node.isNull()) {
            return null;
        }

        ColumnType synapseType = BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
        if (synapseType == null) {
            LOG.error("No Synapse type found for Bridge type " + bridgeType + ", record ID " + recordId);
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
                    String timestampString = node.textValue();
                    try {
                        DateTime dateTime = DateTime.parse(timestampString);
                        return String.valueOf(dateTime.getMillis());
                    } catch (IllegalArgumentException ex) {
                        // log an error, but throw out malformatted dates and return null
                        LOG.error("Invalid timestamp " + timestampString + " for record ID " + recordId);
                        return null;
                    }
                } else if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case DOUBLE:
                if (node.isNumber()) {
                    return String.valueOf(node.decimalValue());
                }
                return null;
            case FILEHANDLEID:
                // file handles are text nodes, where the text is the attachment ID (which is the S3 Key)
                if (node.isTextual()) {
                    String s3Key = node.textValue();
                    return uploadFromS3ToSynapseFileHandle(tmpDir, projectId, fieldName, bridgeType, s3Key);
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
                if (UploadFieldTypes.INLINE_JSON_BLOB.equals(bridgeType)) {
                    nodeValue = node.toString();
                } else {
                    nodeValue = node.asText();
                }

                if (UploadFieldTypes.CALENDAR_DATE.equals(bridgeType)) {
                    // Validate calendar date format. Log an error and return null if the format is invalid.
                    try {
                        LocalDate.parse(nodeValue);
                    } catch (IllegalArgumentException ex) {
                        LOG.error("Invalid calendar date " + nodeValue + " for record ID " + recordId);
                        return null;
                    }
                }

                Integer maxLength = BRIDGE_TYPE_TO_MAX_LENGTH.get(bridgeType);
                if (maxLength == null) {
                    LOG.error("No max length found for Bridge type " + bridgeType);
                    maxLength = DEFAULT_MAX_LENGTH;
                }
                String sanitizedValue = BridgeExporterUtil.sanitizeString(nodeValue, maxLength, recordId);
                return sanitizedValue;
            default:
                LOG.error("Unexpected Synapse Type " + String.valueOf(synapseType) + " for record ID " + recordId);
                return null;
        }
    }

    /**
     * Downloads the specified health data attachment from S3 and uploads it to Synapse as a file handle. This is a
     * fairly complex component, so it's made public to allow for partial mocking in tests.
     *
     * @param tmpDir
     *         temporary directory to use as scratch space for downloading from S3 and uploading to Synapse
     * @param projectId
     *         synapse project ID to upload
     * @param fieldName
     *         field name of the attachment, used to determine the file handle's file name
     * @param bridgeType
     *         Bridge type, used to help determine the file handle's file extension
     * @param attachmentId
     *         attachment ID, also used as the S3 key into the attachments bucket
     * @return the uploaded Synapse file handle ID
     * @throws IOException
     *         if downloading the attachment from S3 fails
     * @throws SynapseException
     *         if uploading the file handle to Synapse fails
     */
    public String uploadFromS3ToSynapseFileHandle(File tmpDir, String projectId, String fieldName, String bridgeType,
            String attachmentId) throws IOException, SynapseException {
        // Create temp file with unique name based on field name, bridge type, and attachment ID.
        String uniqueFilename = generateFilename(fieldName, bridgeType, attachmentId);
        File tempFile = fileHelper.newFile(tmpDir, uniqueFilename);

        String mimeType = BRIDGE_TYPE_TO_MIME_TYPE.get(bridgeType);
        try {
            // download from S3
            s3Helper.downloadS3File(attachmentBucket, attachmentId, tempFile);

            // upload to Synapse
            FileHandle synapseFileHandle = createFileHandleWithRetry(tempFile, mimeType, projectId);
            return synapseFileHandle.getId();
        } finally {
            // delete temp file
            fileHelper.deleteFile(tempFile);
        }
    }

    // Helper method to generate a unique filename for attachments / file handles.
    // Package-scoped to facilitate testing.
    static String generateFilename(String fieldName, String bridgeType, String attachmentId) {
        // Determine file name and extension.
        String fileBaseName, fileExt;
        if (UploadFieldTypes.ATTACHMENT_JSON_BLOB.equals(bridgeType) ||
                UploadFieldTypes.ATTACHMENT_JSON_TABLE.equals(bridgeType)) {
            fileExt = ".json";
            fileBaseName = removeFileExtensionIfPresent(fieldName, fileExt);
        } else if (UploadFieldTypes.ATTACHMENT_CSV.equals(bridgeType)) {
            fileExt = ".csv";
            fileBaseName = removeFileExtensionIfPresent(fieldName, fileExt);
        } else {
            int dotIndex = fieldName.lastIndexOf('.');
            if (dotIndex > 0 && dotIndex < fieldName.length() - 1) {
                // If there's a dot and it's not the first or last char. (That is, there's a dot separating the base
                // name and extension.
                fileExt = fieldName.substring(dotIndex);
                fileBaseName = removeFileExtensionIfPresent(fieldName, fileExt);
            } else {
                // Field name has no file extension, so our new name will have no extension. Base name stays the same.
                fileBaseName = fieldName;
                fileExt = "";
            }
        }

        // File name with pattern [fileBaseName]-[attachmentId][fileExt]. This guarantees filename uniqueness and
        // allows us to have a useful file extension, for OSes that still depend on file extension.
        // Note that fileExt already includes the dot.
        return fileBaseName + '-' + attachmentId + fileExt;
    }

    // Helper method which removes the extension from a filename, unless it doesn't have the extension.
    private static String removeFileExtensionIfPresent(String filename, String fileExt) {
        if (!filename.endsWith(fileExt)) {
            // Easy case: no extension, return as is.
            return filename;
        } else {
            return filename.substring(0, filename.length() - fileExt.length());
        }
    }

    /**
     * Takes a TSV file from disk and uploads and applies its rows to a Synapse table.
     *
     * @param projectId
     *         Synapse project ID that the table lives in
     * @param tableId
     *         Synapse table ID to upload the TSV to
     * @param file
     *         TSV file to apply to the table
     * @return number of rows processed
     * @throws BridgeExporterException
     *         if there's a general error with Bridge EX
     * @throws IOException
     *         if there's an error uploading the file handle
     * @throws SynapseException
     *         if there's an error calling Synapse
     */
    public long uploadTsvFileToTable(String projectId, String tableId, File file) throws BridgeExporterException,
            IOException, SynapseException {
        // Upload TSV as a file handle.
        FileHandle tableFileHandle = createFileHandleWithRetry(file, "text/tab-separated-values", projectId);
        String fileHandleId = tableFileHandle.getId();

        // start tsv import
        CsvTableDescriptor tableDesc = new CsvTableDescriptor();
        tableDesc.setIsFirstLineHeader(true);
        tableDesc.setSeparator("\t");
        String jobToken = uploadTsvStartWithRetry(tableId, fileHandleId, tableDesc);

        // poll asyncGet until success or timeout
        boolean success = false;
        Long linesProcessed = null;
        for (int loops = 0; loops < asyncTimeoutLoops; loops++) {
            if (asyncIntervalMillis > 0) {
                try {
                    Thread.sleep(asyncIntervalMillis);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // poll
            UploadToTableResult uploadResult = getUploadTsvStatus(jobToken, tableId);
            if (uploadResult != null) {
                linesProcessed = uploadResult.getRowsProcessed();
                success = true;
                break;
            }

            // Result not ready. Loop around again.
        }

        if (!success) {
            throw new BridgeExporterException("Timed out uploading file handle " + fileHandleId);
        }
        if (linesProcessed == null) {
            // Not sure if Synapse will ever do this, but code defensively, just in case.
            throw new BridgeExporterException("Null rows processed");
        }

        return linesProcessed;
    }

    /**
     * Appends the given row set to the given Synapse table. This is a retry wrapper.
     *
     * @param rowSet
     *         row set to append
     * @param tableId
     *         Synapse table to appy it to
     * @throws InterruptedException
     *         if the async call is interrupted
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS,
            types = { InterruptedException.class, SynapseException.class }, randomize = false)
    public void appendRowsToTableWithRetry(AppendableRowSet rowSet, String tableId) throws InterruptedException,
            SynapseException {
        synapseClient.appendRowsToTable(rowSet, APPEND_TIMEOUT_MILLISECONDS, tableId);
    }

    /**
     * Creates an ACL in Synapse. This is a retry wrapper.
     *
     * @param acl
     *         ACL to create
     * @return created ACL
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList createAclWithRetry(AccessControlList acl) throws SynapseException {
        return synapseClient.createACL(acl);
    }

    /**
     * Creates column models in Synapse. This is a retry wrapper.
     *
     * @param columnList
     *         list of column models to create
     * @return created column models
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> createColumnModelsWithRetry(List<ColumnModel> columnList) throws SynapseException {
        return synapseClient.createColumnModels(columnList);
    }

    /**
     * Uploads a file to Synapse as a file handle. This is a retry wrapper.
     *
     * @param file
     *         file to upload
     * @param contentType
     *         file MIME type
     * @param projectId
     *         Synapse project to upload the file handle to
     * @return file handle object from Synapse
     * @throws IOException
     *         if reading the file from disk fails
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 1, unit = TimeUnit.SECONDS,
            types = { AmazonClientException.class, SynapseException.class }, randomize = false)
    public FileHandle createFileHandleWithRetry(File file, String contentType, String projectId) throws IOException,
            SynapseException {
        return synapseClient.createFileHandle(file, contentType, projectId);
    }

    /**
     * Helper method to create a table with the specified columns and set up ACLs. The data access team is set with
     * read permissions and the principal ID is set with all permissions.
     *
     * @param columnList
     *         list of column models to create on the table
     * @param dataAccessTeamId
     *         data access team ID, set with read permissions
     * @param principalId
     *         principal ID, set with all permissions
     * @param projectId
     *         Synapse project to create the table in
     * @param tableName
     *         table name
     * @return Synapse table ID
     * @throws BridgeExporterException
     *         under unexpected circumstances, like a table created with the wrong number of columns
     * @throws SynapseException
     *         if the underlying Synapse calls fail
     */
    public String createTableWithColumnsAndAcls(List<ColumnModel> columnList, long dataAccessTeamId,
            long principalId, String projectId, String tableName) throws BridgeExporterException, SynapseException {
        // Create columns
        List<ColumnModel> createdColumnList = createColumnModelsWithRetry(columnList);
        if (columnList.size() != createdColumnList.size()) {
            throw new BridgeExporterException("Error creating Synapse table " + tableName + ": Tried to create " +
                    columnList.size() + " columns. Actual: " + createdColumnList.size() + " columns.");
        }

        List<String> columnIdList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            columnIdList.add(oneCreatedColumn.getId());
        }

        // create table
        TableEntity synapseTable = new TableEntity();
        synapseTable.setName(tableName);
        synapseTable.setParentId(projectId);
        synapseTable.setColumnIds(columnIdList);
        TableEntity createdTable = createTableWithRetry(synapseTable);
        String synapseTableId = createdTable.getId();

        // create ACLs
        // ResourceAccess is a mutable object, but the Synapse API takes them in a Set. This is a little weird.
        // IMPORTANT: Do not modify ResourceAccess objects after adding them to the set. This will break the set.
        Set<ResourceAccess> resourceAccessSet = new HashSet<>();

        ResourceAccess exporterOwnerAccess = new ResourceAccess();
        exporterOwnerAccess.setPrincipalId(principalId);
        exporterOwnerAccess.setAccessType(ACCESS_TYPE_ALL);
        resourceAccessSet.add(exporterOwnerAccess);

        ResourceAccess dataAccessTeamAccess = new ResourceAccess();
        dataAccessTeamAccess.setPrincipalId(dataAccessTeamId);
        dataAccessTeamAccess.setAccessType(ACCESS_TYPE_READ);
        resourceAccessSet.add(dataAccessTeamAccess);

        AccessControlList acl = new AccessControlList();
        acl.setId(synapseTableId);
        acl.setResourceAccess(resourceAccessSet);
        createAclWithRetry(acl);

        return synapseTableId;
    }

    /**
     * Create table in Synapse. This is a retry wrapper.
     *
     * @param table
     *         table to create
     * @return created table
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity createTableWithRetry(TableEntity table) throws SynapseException {
        return synapseClient.createEntity(table);
    }

    /**
     * Download file handle from Synapse. This is a retry wrapper.
     *
     * @param fileHandleId
     *         file handle to download
     * @param toFile
     *         File on local disk to write to
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public void downloadFileHandleWithRetry(String fileHandleId, File toFile) throws SynapseException {
        synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, toFile);
    }

    /**
     * Get the column models for a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         table to get column info for
     * @return list of columns
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> getColumnModelsForTableWithRetry(String tableId) throws SynapseException {
        return synapseClient.getColumnModelsForTableEntity(tableId);
    }

    /**
     * Gets a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         table to get
     * @return table
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity getTableWithRetry(String tableId) throws SynapseException {
        return synapseClient.getEntity(tableId, TableEntity.class);
    }

    /**
     * Updates a Synapse table and returns the updated table. This is a retry wrapper.
     *
     * @param table
     *         table to update
     * @return updated table
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity updateTableWithRetry(TableEntity table) throws SynapseException {
        return synapseClient.putEntity(table);
    }

    /**
     * Starts applying an uploaded TSV file handle to a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         the table to apply the TSV to
     * @param fileHandleId
     *         the TSV file handle
     * @param tableDescriptor
     *         TSV table descriptor
     * @return an async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String uploadTsvStartWithRetry(String tableId, String fileHandleId, CsvTableDescriptor tableDescriptor)
            throws SynapseException {
        return synapseClient.uploadCsvToTableAsyncStart(tableId, fileHandleId, null, null, tableDescriptor);
    }

    /**
     * Polls Synapse to get the job status for the upload TSV to table job. If the job is not ready, this will return
     * null instead of throwing a SynapseResultNotReadyException. This is to prevent spurious retries when a
     * SynapseResultNotReadyException is thrown. This is a retry wrapper.
     *
     * @param jobToken
     *         job token from uploadTsvStartWithRetry()
     * @param tableId
     *         table the job was working on
     * @return upload table result object
     * @throws SynapseException
     *         if the job fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public UploadToTableResult getUploadTsvStatus(String jobToken, String tableId) throws SynapseException {
        try {
            return synapseClient.uploadCsvToTableAsyncGet(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }
}
