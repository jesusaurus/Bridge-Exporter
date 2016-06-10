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
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldDefinition;
import org.sagebionetworks.bridge.sdk.models.upload.UploadFieldType;

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
    private static final Map<UploadFieldType, Integer> BRIDGE_TYPE_TO_MAX_LENGTH =
            ImmutableMap.<UploadFieldType, Integer>builder()
                    .put(UploadFieldType.CALENDAR_DATE, 10)
                    .put(UploadFieldType.DURATION_V2, 24)
                    .put(UploadFieldType.TIME_V2, 12)
                    .build();

    /** Default max length for string columns in Synapse, if the mapping is absent from BRIDGE_TYPE_TO_MAX_LENGTH. */
    private static final int DEFAULT_MAX_LENGTH = 100;

    /**
     * Mapping from attachment types to file extensions. This helps generate a more user-friendly attachment filename.
     */
    private static final Map<UploadFieldType, String> BRIDGE_TYPE_TO_FILE_EXTENSION =
            ImmutableMap.<UploadFieldType, String>builder()
                    .put(UploadFieldType.ATTACHMENT_CSV, ".csv")
                    .put(UploadFieldType.ATTACHMENT_JSON_BLOB, ".json")
                    .put(UploadFieldType.ATTACHMENT_JSON_TABLE, ".json")
                    .build();

    /** Mapping from attachment types to MIME types, for use with telling Synapse what kind of file handle this is. */
    private static final Map<UploadFieldType, String> BRIDGE_TYPE_TO_MIME_TYPE =
            ImmutableMap.<UploadFieldType, String>builder()
                    .put(UploadFieldType.ATTACHMENT_CSV, "text/csv")
                    .put(UploadFieldType.ATTACHMENT_JSON_BLOB, "text/json")
                    .put(UploadFieldType.ATTACHMENT_JSON_TABLE, "text/json")
                    .build();

    /** Default MIME type. application/octet-stream is used for arbitrary (potentially binary) data. */
    private static final String DEFAULT_MIME_TYPE = "application/octet-stream";

    /**
     * Mapping from Bridge schema column types to Synapse table column types. Excludes types that map to multiple
     * Synapse columns, such as MULTI_CHOICE or TIMESTAMP.
     */
    public static final Map<UploadFieldType, ColumnType> BRIDGE_TYPE_TO_SYNAPSE_TYPE =
            ImmutableMap.<UploadFieldType, ColumnType>builder()
                    .put(UploadFieldType.ATTACHMENT_BLOB, ColumnType.FILEHANDLEID)
                    .put(UploadFieldType.ATTACHMENT_CSV, ColumnType.FILEHANDLEID)
                    .put(UploadFieldType.ATTACHMENT_JSON_BLOB, ColumnType.FILEHANDLEID)
                    .put(UploadFieldType.ATTACHMENT_JSON_TABLE, ColumnType.FILEHANDLEID)
                    .put(UploadFieldType.ATTACHMENT_V2, ColumnType.FILEHANDLEID)
                    .put(UploadFieldType.BOOLEAN, ColumnType.BOOLEAN)
                    .put(UploadFieldType.CALENDAR_DATE, ColumnType.STRING)
                    .put(UploadFieldType.DURATION_V2, ColumnType.STRING)
                    .put(UploadFieldType.FLOAT, ColumnType.DOUBLE)
                    .put(UploadFieldType.INLINE_JSON_BLOB, ColumnType.STRING)
                    .put(UploadFieldType.INT, ColumnType.INTEGER)
                    .put(UploadFieldType.SINGLE_CHOICE, ColumnType.STRING)
                    .put(UploadFieldType.STRING, ColumnType.STRING)
                    .put(UploadFieldType.TIME_V2, ColumnType.STRING)
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
     * <p>
     * Serializes a Bridge health data record column into a Synapse table column.
     * </p>
     * <p>
     * This should not be called for MULTI_CHOICE or TIMESTAMP types, as these types serialize into multiple columns.
     * This method is intended only for fields that serialize into a single column.
     * </p>
     *
     * @param tmpDir
     *         temp directory, used for scratch space for uploading attachments
     * @param projectId
     *         Synapse project ID, used to determine where to upload attachments to
     * @param recordId
     *         Bridge record ID, used for logging
     * @param fieldDef
     *         field definition, to determine how to serialize the field
     * @param node
     *         value to serialize
     * @return serialized value, to be uploaded to a Synapse table
     * @throws IOException
     *         if downloading the attachment from S3 fails
     * @throws SynapseException
     *         if uploading the attachment to Synapse fails
     */
    public String serializeToSynapseType(File tmpDir, String projectId, String recordId,
            UploadFieldDefinition fieldDef, JsonNode node) throws IOException, SynapseException {
        if (node == null || node.isNull()) {
            return null;
        }

        UploadFieldType fieldType = fieldDef.getType();
        switch (fieldType) {
            case ATTACHMENT_BLOB:
            case ATTACHMENT_CSV:
            case ATTACHMENT_JSON_BLOB:
            case ATTACHMENT_JSON_TABLE:
            case ATTACHMENT_V2: {
                // file handles are text nodes, where the text is the attachment ID (which is the S3 Key)
                if (node.isTextual()) {
                    String s3Key = node.textValue();
                    return uploadFromS3ToSynapseFileHandle(tmpDir, projectId, fieldDef, s3Key);
                }
                return null;
            }
            case BOOLEAN: {
                if (node.isBoolean()) {
                    return String.valueOf(node.booleanValue());
                }
                return null;
            }
            case CALENDAR_DATE:
            case DURATION_V2:
            case INLINE_JSON_BLOB:
            case SINGLE_CHOICE:
            case STRING:
            case TIME_V2: {
                // These types are all strings. Some are fixed length, some are variable length. Some are short enough
                // to use the Synapse String type. Some are too long and need to use the blob (large text) type.
                // Regardless, they all go through the same logic here and are serialized in the TSV as just a string.
                String nodeValue;
                if (node.isTextual()) {
                    nodeValue = node.textValue();
                } else {
                    // Some types (notably INLINE_JSON_BLOB) will use the whole JSON value.
                    nodeValue = node.toString();
                }
                Integer maxLength = getMaxLengthForFieldDef(fieldDef);
                String sanitizedValue = BridgeExporterUtil.sanitizeString(nodeValue, maxLength, recordId);
                return sanitizedValue;
            }
            case FLOAT: {
                if (node.isNumber()) {
                    return String.valueOf(node.decimalValue());
                }
                return null;
            }
            case INT: {
                if (node.isNumber()) {
                    return String.valueOf(node.bigIntegerValue());
                }
                return null;
            }
            default:
                LOG.error("Unexpected type " + fieldType.name() + " for record ID " + recordId);
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
     * @param fieldDef
     *         field definition, used to determine file name, extension, and MIME type
     * @param attachmentId
     *         attachment ID, also used as the S3 key into the attachments bucket
     * @return the uploaded Synapse file handle ID
     * @throws IOException
     *         if downloading the attachment from S3 fails
     * @throws SynapseException
     *         if uploading the file handle to Synapse fails
     */
    public String uploadFromS3ToSynapseFileHandle(File tmpDir, String projectId, UploadFieldDefinition fieldDef,
            String attachmentId) throws IOException, SynapseException {
        // Create temp file with unique name based on field name, bridge type, and attachment ID.
        String uniqueFilename = generateFilename(fieldDef, attachmentId);
        File tempFile = fileHelper.newFile(tmpDir, uniqueFilename);
        String mimeType = getMimeTypeForFieldDef(fieldDef);

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
    static String generateFilename(UploadFieldDefinition fieldDef, String attachmentId) {
        // File name with pattern [fileBaseName]-[attachmentId][fileExt]. This guarantees filename uniqueness and
        // allows us to have a useful file extension, for OSes that still depend on file extension.

        String fieldName = fieldDef.getName();

        // Check field def to determine file extension first.
        String fileExt = null;
        String defFileExt = fieldDef.getFileExtension();
        if (defFileExt != null) {
            fileExt = defFileExt;
        }

        // Fall back to defaults per field type.
        if (fileExt == null) {
            String typeFileExt = BRIDGE_TYPE_TO_FILE_EXTENSION.get(fieldDef.getType());
            if (typeFileExt != null) {
                fileExt = typeFileExt;
            }
        }

        // Fall back to the file's own file extension.
        if (fileExt == null) {
            // If there's a dot and it's not the first or last char. (That is, there's a dot separating the base name
            // and extension.)
            int dotIndex = fieldName.lastIndexOf('.');
            if (dotIndex > 0 && dotIndex < fieldName.length() - 1) {
                fileExt = fieldName.substring(dotIndex);
            }
        }

        if (fileExt != null) {
            // If we have a file extension, remove it from the from the base name, or we end up with things like
            // foo.json-attachmentId.json.
            String fileBaseName = removeFileExtensionIfPresent(fieldName, fileExt);

            // Note that fileExt already includes the dot.
            return fileBaseName + '-' + attachmentId + fileExt;
        } else {
            return fieldName + '-' + attachmentId;
        }
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
     * Helper method to get the MIME type for the given field definitions, using the field definitions parameters and
     * its type.
     */
    private static String getMimeTypeForFieldDef(UploadFieldDefinition fieldDef) {
        // First try MIME type defined in the field def.
        String defMimeType = fieldDef.getMimeType();
        if (defMimeType != null) {
            return defMimeType;
        }

        // Fall back to type specific MIME types.
        String typeMimeType = BRIDGE_TYPE_TO_MIME_TYPE.get(fieldDef.getType());
        if (typeMimeType != null) {
            return typeMimeType;
        }

        // Fall back to global default.
        return DEFAULT_MIME_TYPE;
    }

    /**
     * Helper method to get the max string length for the given field definitions, using the field definitions
     * parameters and its type.
     */
    public static int getMaxLengthForFieldDef(UploadFieldDefinition fieldDef) {
        // First try max length defined in field def.
        Integer defMaxLength = fieldDef.getMaxLength();
        if (defMaxLength != null) {
            return defMaxLength;
        }

        // Fall back to type specific max length.
        Integer typeMaxLength = BRIDGE_TYPE_TO_MAX_LENGTH.get(fieldDef.getType());
        if (typeMaxLength != null) {
            return typeMaxLength;
        }

        // Fall back to global default.
        return DEFAULT_MAX_LENGTH;
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
