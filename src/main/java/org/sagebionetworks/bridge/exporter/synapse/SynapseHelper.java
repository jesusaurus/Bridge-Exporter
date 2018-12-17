package org.sagebionetworks.bridge.exporter.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.RateLimiter;
import com.jcabi.aspects.RetryOnFailure;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.sagebionetworks.repo.model.table.TableSchemaChangeResponse;
import org.sagebionetworks.repo.model.table.TableUpdateRequest;
import org.sagebionetworks.repo.model.table.TableUpdateResponse;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterNonRetryableException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

/** Helper class for Synapse calls, including complex logic around asynchronous calls and retry helper. */
@Component
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    private static final long APPEND_TIMEOUT_MILLISECONDS = 30 * 1000;

    // Config keys. Package-scoped to allow unit tests to mock.
    static final String CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS = "synapse.async.interval.millis";
    static final String CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS = "synapse.async.timeout.loops";
    static final String CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND = "synapse.rate.limit.per.second";
    static final String CONFIG_KEY_SYNAPSE_GET_COLUMN_MODELS_RATE_LIMIT_PER_MINUTE =
            "synapse.get.column.models.rate.limit.per.minute";

    // Shared constants.
    public static final Set<ACCESS_TYPE> ACCESS_TYPE_ALL = ImmutableSet.copyOf(ACCESS_TYPE.values());
    public static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);
    public static final String DDB_TABLE_SYNAPSE_META_TABLES = "SynapseMetaTables";
    public static final String DDB_KEY_TABLE_NAME = "tableName";

    // Map of allowed column type changes. Key is the old type. Value is the new type.
    //
    // A few notes: This is largely based on whether the data can be converted in Synapse tables. Since booleans are
    // stored as 0/1 and dates are stored as epoch milliseconds, converting these to strings means old values will be
    // numeric types, but new values are likely to be "true"/"false" or ISO8601 timestamps. This leads to more
    // confusion overall, so we've decided to block it.
    //
    // Similarly, if you convert a bool to a numeric type (int, float), Synapse will convert the bools to 0s and 1s.
    // However, old bools in DynamoDB are still using "true"/"false", which will no longer serialize to Synapse. To
    // prevent this data loss, we're also not allowing bools to convert to numeric types.
    private static final SetMultimap<ColumnType, ColumnType> ALLOWED_OLD_TYPE_TO_NEW_TYPE =
            ImmutableSetMultimap.<ColumnType, ColumnType>builder()
                    // Numeric types can changed to types with more precision (int to float), but not less
                    // precision (float to int).
                    .put(ColumnType.INTEGER, ColumnType.DOUBLE)
                    // Date can be converted to int and float (epoch milliseconds), and can be converted back from an
                    // int. However, we block converting from float, since that causes data loss.
                    .put(ColumnType.DATE, ColumnType.INTEGER)
                    .put(ColumnType.DATE, ColumnType.DOUBLE)
                    .put(ColumnType.INTEGER, ColumnType.DATE)
                    // Numeric types are trivially converted to strings.
                    .put(ColumnType.DOUBLE, ColumnType.STRING)
                    .put(ColumnType.INTEGER, ColumnType.STRING)
                    .put(ColumnType.DOUBLE, ColumnType.LARGETEXT)
                    .put(ColumnType.INTEGER, ColumnType.LARGETEXT)
                    // Strings can be converted into bigger strings.
                    .put(ColumnType.STRING, ColumnType.LARGETEXT)
                    .build();

    /**
     * Mapping from Bridge types to their respective max lengths. This is used when we have a Bridge type that
     * serializes as a String in Synapse, and we want to determine a max length for the String column.
     */
    private static final Map<UploadFieldType, Integer> BRIDGE_TYPE_TO_MAX_LENGTH =
            ImmutableMap.<UploadFieldType, Integer>builder()
                    .put(UploadFieldType.CALENDAR_DATE, 10)
                    .put(UploadFieldType.DURATION_V2, 24)
                    .put(UploadFieldType.TIME_V2, 12)
                    .build();

    /** Default max length for Bridge columns in Synapse. */
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
                    .put(UploadFieldType.LARGE_TEXT_ATTACHMENT, ColumnType.LARGETEXT)
                    .put(UploadFieldType.SINGLE_CHOICE, ColumnType.STRING)
                    .put(UploadFieldType.STRING, ColumnType.STRING)
                    .put(UploadFieldType.TIME_V2, ColumnType.STRING)
                    .build();

    /**
     * The max lengths of various Synapse column types. This is used to determine how long a Synapse column can be
     * when we convert the column to a String. This only contains column types that can be converted to Strings. Note
     * that it excludes dates and bools, as Synapse considers these numeric types, but Bridge uses ISO8601 and
     * "true"/"false".
     */
    private static final Map<ColumnType, Integer> SYNAPSE_TYPE_TO_MAX_LENGTH =
            ImmutableMap.<ColumnType, Integer>builder()
                    // Empirically, the longest float in Synapse is 22 chars long
                    .put(ColumnType.DOUBLE, 22)
                    // Synapse uses a bigint (signed long), which can be 20 chars long
                    .put(ColumnType.INTEGER, 20)
                    .build();

    // config
    private int asyncIntervalMillis;
    private int asyncTimeoutLoops;
    private String attachmentBucket;

    // Spring helpers
    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private SynapseClient synapseClient;

    // Rate limiter, used to limit the amount of traffic to Synapse. Synapse throttles at 10 requests per second.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    // Rate limiter for getColumnModelsForEntity(). This is rate limited to 6 per minute per host, for each of 8 hosts,
    // for a total of 48 calls per minute. Add a safety factor and rate limit to 24 per minute.
    private final RateLimiter getColumnModelsRateLimiter = RateLimiter.create(24.0 / 60.0);

    /** Config, used to get the attachment S3 bucket to get Bridge attachments. */
    @Autowired
    public final void setConfig(Config config) {
        this.asyncIntervalMillis = config.getInt(CONFIG_KEY_SYNAPSE_ASYNC_INTERVAL_MILLIS);
        this.asyncTimeoutLoops = config.getInt(CONFIG_KEY_SYNAPSE_ASYNC_TIMEOUT_LOOPS);
        this.attachmentBucket = config.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET);

        int rateLimitPerSecond = config.getInt(CONFIG_KEY_SYNAPSE_RATE_LIMIT_PER_SECOND);
        rateLimiter.setRate(rateLimitPerSecond);

        int getColumnModelsRateLimitPerMinute = config.getInt(
                CONFIG_KEY_SYNAPSE_GET_COLUMN_MODELS_RATE_LIMIT_PER_MINUTE);
        getColumnModelsRateLimiter.setRate(getColumnModelsRateLimitPerMinute / 60.0);
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
     * Returns true if the old column can be converted to the new column in a meaningful way without data loss. Used to
     * determine if the schema changes, whether BridgeEX should try to modify the table.
     *
     * @param oldColumn
     *         column model currently in Synapse
     * @param newColumn
     *         column model we want to replace it with
     * @return true if they're compatible, false otherwise
     * @throws BridgeExporterException
     *         if there's an unexpected error comparing the columns
     */
    public static boolean isCompatibleColumn(ColumnModel oldColumn, ColumnModel newColumn)
            throws BridgeExporterException {
        // Ignore the following attributes:
        // ID - The ones from Synapse will have IDs, but the ones we generate from the schema won't. This is normal.
        // defaultValues, enumValues - We don't use these, and changing these won't affect data integrity.

        // If types are different, check the table.
        if (oldColumn.getColumnType() != newColumn.getColumnType()) {
            Set<ColumnType> allowedNewTypes = ALLOWED_OLD_TYPE_TO_NEW_TYPE.get(oldColumn.getColumnType());
            if (!allowedNewTypes.contains(newColumn.getColumnType())) {
                return false;
            }
        }

        // For string types, check max length. You can increase the max length, but you can't decrease it.
        if (newColumn.getColumnType() == ColumnType.STRING &&
                !Objects.equals(oldColumn.getMaximumSize(), newColumn.getMaximumSize())) {
            long oldMaxLength;
            if (oldColumn.getMaximumSize() != null) {
                // If the old field def specified a max length, just use it.
                oldMaxLength = oldColumn.getMaximumSize();
            } else if (SYNAPSE_TYPE_TO_MAX_LENGTH.containsKey(oldColumn.getColumnType())) {
                // The max length of the old field type is specified by its type.
                oldMaxLength = SYNAPSE_TYPE_TO_MAX_LENGTH.get(oldColumn.getColumnType());
            } else {
                // This should never happen. If we get here, that means we somehow have a String column in Synapse
                // without a Max Length parameter. Abort and throw.
                throw new BridgeExporterNonRetryableException("old column " + oldColumn.getName() + " has type " +
                        oldColumn.getColumnType() + " and no max length");
            }

            if (newColumn.getMaximumSize() == null) {
                // This should also never happen. This means that we generated a String column without a Max Length,
                // which is bad.
                throw new BridgeExporterNonRetryableException("new column " + newColumn.getName() + " has type " +
                        "STRING and no max length");
            }
            long newMaxLength = newColumn.getMaximumSize();

            // You can't decrease max length.
            if (newMaxLength < oldMaxLength) {
                return false;
            }
        }

        // This should never happen, but if the names are somehow different, they aren't compatible.
        //noinspection RedundantIfStatement
        if (!Objects.equals(oldColumn.getName(), newColumn.getName())) {
            return false;
        }

        // If we passed all incompatibility checks, then we're compatible.
        return true;
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
     * @param studyId
     *         Bridge study ID, used for logging
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
    public String serializeToSynapseType(Metrics metrics, File tmpDir, String projectId, String recordId,
            String studyId, UploadFieldDefinition fieldDef, JsonNode node) throws IOException, SynapseException {
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
                    // We want to count the number of attachments we upload to Synapse, since this is the biggest source of
                    // Synapse traffic from us.
                    metrics.incrementCounter("numAttachments");

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

                Boolean isUnboundedText = fieldDef.getUnboundedText();
                Integer maxLength = null;
                if (isUnboundedText == null || !isUnboundedText) {
                    maxLength = getMaxLengthForFieldDef(fieldDef);
                }

                String sanitizedValue = BridgeExporterUtil.sanitizeString(nodeValue, fieldDef.getName(), maxLength,
                        recordId, studyId);
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
            case LARGE_TEXT_ATTACHMENT: {
                // This is stored in Bridge as an attachment. The JSON node is text, which is the attachment ID (which
                // is also the S3 key).
                if (node.isTextual()) {
                    // We need to upload this to Synapse as a LargeText inlined in the table. Download the file content
                    // as a string.
                    String value = downloadLargeTextAttachment(node.textValue());

                    // We also need to sanitize the content (remove HTML, newlines, tabs, quote strings, etc).
                    String sanitizedValue = BridgeExporterUtil.sanitizeString(value, fieldDef.getName(), null,
                            recordId, studyId);
                    return sanitizedValue;
                }
                return null;
            }
            default:
                LOG.error("Unexpected type " + fieldType.name() + " for record ID " + recordId);
                return null;
        }
    }

    /**
     * Helper method to download a large text attachment from S3. This is a separate public helper method to facilitate
     * mocking and spying in unit tests, and should never be called directly.
     */
    public String downloadLargeTextAttachment(String attachmentId) throws IOException {
        return s3Helper.readS3FileAsString(attachmentBucket, attachmentId);
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
     * @return the uploaded Synapse file handle ID, or null if no file handle was created
     * @throws IOException
     *         if downloading the attachment from S3 fails
     * @throws SynapseException
     *         if uploading the file handle to Synapse fails
     */
    public String uploadFromS3ToSynapseFileHandle(File tmpDir, String projectId, UploadFieldDefinition fieldDef,
            String attachmentId) throws IOException, SynapseException {
        // Create temp file with unique name based on field name, bridge type, and attachment ID.
        String uniqueFilename = generateFilename(fieldDef, attachmentId);
        return uploadFromS3ToSynapseFileHandle(tmpDir, uniqueFilename, attachmentId);
    }

    /**
     * Downloads the specified health data attachment and uploads it to Synapse as a file handle, using the specified
     * filename.
     */
    public String uploadFromS3ToSynapseFileHandle(File tmpDir, String filename, String attachmentId)
            throws IOException, SynapseException {
        File tempFile = fileHelper.newFile(tmpDir, filename);
        try {
            // download from S3
            s3Helper.downloadS3File(attachmentBucket, attachmentId, tempFile);

            // don't upload empty files
            if (fileHelper.fileSize(tempFile) == 0) {
                return null;
            }

            // upload to Synapse
            FileHandle synapseFileHandle = createFileHandleWithRetry(tempFile);
            return synapseFileHandle.getId();
        } finally {
            // delete temp file
            fileHelper.deleteFile(tempFile);
        }
    }

    // Helper method to generate a unique filename for attachments / file handles.
    // Package-scoped to facilitate testing.
    static String generateFilename(UploadFieldDefinition fieldDef, String attachmentId) {
        String fieldName = fieldDef.getName();

        // Newer attachment IDs are already in the the form [uploadId]-[fieldName]. These are uniquely named well
        // enough that we can use them as is.
        if (attachmentId.endsWith(fieldName)) {
            return attachmentId;
        }

        // Legacy uploads:
        // File name with pattern [fileBaseName]-[attachmentId][fileExt]. This guarantees filename uniqueness and
        // allows us to have a useful file extension, for OSes that still depend on file extension.

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
     * Updates table columns.
     *
     * @param schemaChangeRequest
     *         requested change
     * @param tableId
     *         table to update
     * @return table change response
     * @throws BridgeExporterException
     *         if there's a general error with Bridge EX
     * @throws SynapseException
     *         if there's an error calling Synapse
     */
    public TableSchemaChangeResponse updateTableColumns(TableSchemaChangeRequest schemaChangeRequest, String tableId)
            throws BridgeExporterException, SynapseException {
        // For convenience, this API only contains a single TableSchemaChangeRequest, but the Synapse API takes a whole
        // list. Wrap it in a list.
        List<TableUpdateRequest> changeList = ImmutableList.of(schemaChangeRequest);

        // Start the table update job.
        String jobToken = startTableTransactionWithRetry(changeList, tableId);

        // Poll async get until success or timeout.
        boolean success = false;
        List<TableUpdateResponse> responseList = null;
        for (int loops = 0; loops < asyncTimeoutLoops; loops++) {
            if (asyncIntervalMillis > 0) {
                try {
                    Thread.sleep(asyncIntervalMillis);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // poll
            responseList = getTableTransactionResultWithRetry(jobToken, tableId);
            if (responseList != null) {
                success = true;
                break;
            }

            // Result not ready. Loop around again.
        }

        if (!success) {
            throw new BridgeExporterException("Timed out updating table columns for table " + tableId);
        }

        // The list should have a single response, and it should be a TableSchemaChangeResponse.
        if (responseList.size() != 1) {
            throw new BridgeExporterException("Expected one table update response for table " + tableId +
                    ", but got " + responseList.size());
        }
        TableUpdateResponse singleResponse = responseList.get(0);
        if (!(singleResponse instanceof TableSchemaChangeResponse)) {
            throw new BridgeExporterException("Expected a TableSchemaChangeResponse for table " + tableId +
                    ", but got " + singleResponse.getClass().getName());
        }

        return (TableSchemaChangeResponse) singleResponse;
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
        FileHandle tableFileHandle = createFileHandleWithRetry(file);
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS,
            types = { InterruptedException.class, SynapseException.class }, randomize = false)
    public void appendRowsToTableWithRetry(AppendableRowSet rowSet, String tableId) throws InterruptedException,
            SynapseException {
        rateLimiter.acquire();
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList createAclWithRetry(AccessControlList acl) throws SynapseException {
        rateLimiter.acquire();
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> createColumnModelsWithRetry(List<ColumnModel> columnList) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createColumnModels(columnList);
    }

    /**
     * Uploads a file to Synapse as a file handle. This is a retry wrapper.
     *
     * @param file
     *         file to upload
     * @return file handle object from Synapse
     * @throws IOException
     *         if reading the file from disk fails
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 2, delay = 1, unit = TimeUnit.SECONDS,
            types = { AmazonClientException.class, SynapseException.class }, randomize = false)
    public FileHandle createFileHandleWithRetry(File file) throws IOException,
            SynapseException {
        rateLimiter.acquire();
        // Pass in forceRestart=true. Otherwise, retries will fail deterministically.
        return synapseClient.multipartUpload(file, null, null, true);
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity createTableWithRetry(TableEntity table) throws SynapseException {
        rateLimiter.acquire();
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public void downloadFileHandleWithRetry(String fileHandleId, File toFile) throws SynapseException {
        rateLimiter.acquire();
        synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, toFile);
    }

    /**
     * Gets the Synapse stack status and returns true if Synapse is up and in read/write state. Also includes retries.
     *
     * @return true if Synapse is up and in read/write state
     * @throws JSONObjectAdapterException
     *         if the call fails
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public boolean isSynapseWritable() throws JSONObjectAdapterException, SynapseException {
        rateLimiter.acquire();
        StackStatus status = synapseClient.getCurrentStackStatus();
        return status.getStatus() == StatusEnum.READ_WRITE;
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> getColumnModelsForTableWithRetry(String tableId) throws SynapseException {
        getColumnModelsRateLimiter.acquire();
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity getTableWithRetry(String tableId) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.getEntity(tableId, TableEntity.class);
    }

    /**
     * Starts a Synapse table transaction (for example, a schema update request). This is a retry wrapper.
     *
     * @param changeList
     *         changes to apply to table
     * @param tableId
     *         table to be changed
     * @return async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String startTableTransactionWithRetry(List<TableUpdateRequest> changeList, String tableId)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.startTableTransactionJob(changeList, tableId);
    }

    /**
     * Polls Synapse to get the job status for a table transaction (such as a schema update request). If the job is not
     * ready, this will return null instead of throwing a SynapseResultNotReadyException. This is to prevent spurious
     * retries when a SynapseResultNotReadyException is thrown. This is a retry wrapper.
     *
     * @param jobToken
     *         job token from startTableTransactionWithRetry()
     * @param tableId
     *         table the job was working on
     * @return response from the table update
     * @throws SynapseException
     *         if the job fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<TableUpdateResponse> getTableTransactionResultWithRetry(String jobToken, String tableId)
            throws SynapseException {
        try {
            rateLimiter.acquire();
            return synapseClient.getTableTransactionJobResults(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity updateTableWithRetry(TableEntity table) throws SynapseException {
        rateLimiter.acquire();
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String uploadTsvStartWithRetry(String tableId, String fileHandleId, CsvTableDescriptor tableDescriptor)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.uploadCsvToTableAsyncStart(tableId, fileHandleId, null, null, tableDescriptor, null);
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
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public UploadToTableResult getUploadTsvStatus(String jobToken, String tableId) throws SynapseException {
        try {
            rateLimiter.acquire();
            return synapseClient.uploadCsvToTableAsyncGet(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }
}
