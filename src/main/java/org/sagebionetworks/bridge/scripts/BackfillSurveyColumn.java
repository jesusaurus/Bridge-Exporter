package org.sagebionetworks.bridge.scripts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.util.csv.CsvNullReader;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.exporter.ExportHelper;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Usage: BackfillColumn [TSV file] [synapse table ID] [study ID] [schema ID] [schema rev] [[column name]...]
 *
 * TSV file is the TSV file of the table of all rows that need to be backfilled, including ROW_ID and ROW_VERSION.
 * Normally, this should be obtained using a query where [column name] is not null.
 *
 * synapse table ID is the table to be backfilled
 *
 * study ID, schema ID, and schema rev are used to convert the data
 *
 * column name is the name of the column to be backfilled. This is a space separated list of columns.
 */
public class BackfillSurveyColumn {
    private static final Joiner COLUMN_JOINER = Joiner.on('\t').useForNull("");

    // Script should report progress after this many records, so users tailing the logs can see that it's still
    // making progress
    private static final int PROGRESS_REPORT_PERIOD = 1000;
    private static final int TASK_REMAINING_REPORT_PERIOD = 250;

    // TODO: Global vars are bad. Make these non-static and object oriented.

    // inputs
    private static String filename;
    private static String synapseTableId;
    private static String studyId;
    private static UploadSchemaKey schemaKey;
    private static Set<String> columnNameSet;

    // internal state
    private static Map<String, String> columnNameToBridgeType;
    private static Map<String, Integer> columnNameToIndex;
    private static BridgeExporterConfig config;
    private static int lineCount = 0;
    private static File newTsvFile;
    private static PrintWriter newTsvWriter;
    private static CsvNullReader oldFileCsvReader;
    private static Integer recordIdIndex;
    private static UploadSchema schema;

    // services
    private static ExecutorService executor;
    private static ExportHelper exportHelper;
    private static Table recordTable;
    private static SynapseHelper synapseHelper;

    public static void main(String[] args) {
        filename = args[0];
        synapseTableId = args[1];
        studyId = args[2];
        schemaKey = new UploadSchemaKey(studyId, args[3], Integer.parseInt(args[4]));

        ImmutableSet.Builder<String> columnNameSetBuilder = ImmutableSet.builder();
        for (int i = 5; i < args.length; i++) {
            columnNameSetBuilder.add(args[i]);
        }
        columnNameSet = columnNameSetBuilder.build();
        if (columnNameSet.isEmpty()) {
            throw new IllegalArgumentException("empty column name list specified");
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            System.out.println("[METRICS] Starting initialization: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            init();
            System.out.println("[METRICS] Initialization done: " + BridgeExporterUtil.getCurrentLocalTimestamp());

            backfill();
            uploadTsv();
        } catch (Throwable t) {
            // shift err into out, in case we forget to do 2&>1
            t.printStackTrace(System.out);
        } finally {
            stopwatch.stop();
            System.out.println("[METRICS] Wrote " + lineCount + " lines");
            System.out.println("[METRICS] Backfill done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            System.out.println("[METRICS] Total time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            System.exit(0);
        }
    }

    private static void init() throws BridgeExporterException, IOException, SchemaNotFoundException, SynapseException {
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile, BridgeExporterConfig.class);

        // parse file and get indices from headers
        Reader fileReader = new InputStreamReader(new FileInputStream(filename), Charsets.UTF_8);
        oldFileCsvReader = new CsvNullReader(fileReader, '\t');
        String[] headers = oldFileCsvReader.readNext();

        ImmutableMap.Builder<String, Integer> indexMapBuilder = ImmutableMap.builder();
        boolean hasRowId = false;
        boolean hasRowVersion = false;
        for (int i = 0; i < headers.length; i++) {
            String oneHeader = headers[i];
            if (oneHeader.equals("recordId")) {
                recordIdIndex = i;
            } else if (columnNameSet.contains(oneHeader)) {
                indexMapBuilder.put(oneHeader, i);
            } else if (oneHeader.equals("ROW_ID")) {
                hasRowId = true;
            } else if (oneHeader.equals("ROW_VERSION")) {
                hasRowVersion = true;
            }
        }
        if (recordIdIndex == null) {
            throw new BridgeExporterException("No recordId column in input file");
        }
        columnNameToIndex = indexMapBuilder.build();
        if (!columnNameSet.equals(columnNameToIndex.keySet())) {
            throw new BridgeExporterException("Header is missing columns");
        }
        if (!hasRowId) {
            throw new BridgeExporterException("No ROW_ID column in input file");
        }
        if (!hasRowVersion) {
            throw new BridgeExporterException("No ROW_VERSION column in input file");
        }

        // new TSV file and writer
        newTsvFile = File.createTempFile(filename + "-backfilled", ".tsv");
        OutputStream stream = new FileOutputStream(newTsvFile);
        newTsvWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream, Charsets.UTF_8)));

        // Write headers. It's the same headers.
        newTsvWriter.println(COLUMN_JOINER.join(headers));

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        recordTable = ddbClient.getTable("prod-heroku-HealthDataRecord3");

        // schema Helper
        UploadSchemaHelper schemaHelper = new UploadSchemaHelper();
        schemaHelper.setDdbClient(ddbClient);
        schemaHelper.init();

        schema = schemaHelper.getSchema(schemaKey);
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        ImmutableMap.Builder<String, String> bridgeTypeMapBuilder = ImmutableMap.builder();
        for (String oneColumnName : columnNameSet) {
            String bridgeType = fieldTypeMap.get(oneColumnName);
            bridgeTypeMapBuilder.put(oneColumnName, bridgeType);
        }
        columnNameToBridgeType = bridgeTypeMapBuilder.build();

        // executor
        executor = Executors.newFixedThreadPool(config.getNumThreads());

        // S3 client - move to Spring
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // Export Helper
        exportHelper = new ExportHelper();
        exportHelper.setDdbClient(ddbClient);
        exportHelper.setS3Helper(s3Helper);

        // synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // because of a bug in the Java client, we need to properly log in to upload file handles
        // see https://sagebionetworks.jira.com/browse/PLFM-3310
        synapseClient.login(config.getUsername(), config.getPassword());

        // synapse helper - for this use case, we only need the synapse client
        synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(synapseClient);
    }

    private static void backfill() throws IOException {
        // Loop over old lines.
        // First line is the header, so discard
        Stopwatch submitTaskStopwatch = Stopwatch.createStarted();
        int ddbDelay = config.getDdbDelay();
        Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();

        String[] columns;
        int lineNum = 0;
        while ((columns = oldFileCsvReader.readNext()) != null) {
            if (ddbDelay > 0) {
                // sleep to rate limit our requests to DDB
                try {
                    Thread.sleep(ddbDelay);
                } catch (InterruptedException ex) {
                    System.out.println("[ERROR] Interrupted while sleeping: " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }

            lineNum++;
            Future<?> taskFuture = executor.submit(new Task(lineNum, columns));
            outstandingTaskQueue.add(taskFuture);

            if (lineNum % PROGRESS_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num records so far: " + lineNum + " in "
                        + submitTaskStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // wait on outstanding tasks
        System.out.println("[METRICS] Done submitting tasks: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        Stopwatch outstandingTaskStopwatch = Stopwatch.createStarted();
        while (!outstandingTaskQueue.isEmpty()) {
            int numOutstanding = outstandingTaskQueue.size();
            if (numOutstanding % TASK_REMAINING_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num outstanding tasks: " + numOutstanding + " after " +
                        outstandingTaskStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }

            // ExportWorkers have no return value. If Future.get() returns normally, then the task is done.
            Future<?> oneFuture = outstandingTaskQueue.remove();
            try {
                oneFuture.get();
            } catch (ExecutionException | InterruptedException ex) {
                System.out.println("[ERROR] Error finishing task: " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        System.out.println("[METRICS] All tasks done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
    }

    private static class Task implements Runnable {
        private final int lineNum;
        private final String[] columns;

        public Task(int lineNum, String[] columns) {
            this.lineNum = lineNum;
            this.columns = columns;
        }

        @Override
        public void run() {
            try {
                processRecord(columns);
            } catch (Throwable t) {
                throw new RuntimeException("Error processing line " + lineNum + ": " + t.getMessage(), t);
            }
        }
    }

    private static void processRecord(String[] columns) throws BridgeExporterException, IOException {
        // get record
        String recordId = columns[recordIdIndex];
        Item record = recordTable.getItem("id", recordId);

        // convert record to health data node
        JsonNode convertedSurveyNode = exportHelper.convertSurveyRecordToHealthDataJsonNode(record, schema);

        boolean modified = false;
        for (String columnName : columnNameSet) {
            int columnIndex = columnNameToIndex.get(columnName);
            String bridgeType = columnNameToBridgeType.get(columnName);

            // get column from JSON and inject it into the columns we already have
            JsonNode columnValueNode = convertedSurveyNode.get(columnName);
            String value = synapseHelper.serializeToSynapseType(studyId, recordId, columnName, bridgeType,
                    columnValueNode);
            if (!Strings.isNullOrEmpty(value)) {
                columns[columnIndex] = value;
                modified = true;
            }
        }

        // if we modified anything, write the new row
        if (modified) {
            String newRow = COLUMN_JOINER.join(columns);

            // Write the new row back to the TSV writer. We need to synchronize on the TSV writer to make sure we
            // don'
            // conflict with other threads
            synchronized (newTsvWriter) {
                newTsvWriter.println(newRow);
                lineCount++;
            }
        }
    }

    private static void uploadTsv() throws BridgeExporterException {
        // flush and close writer, check for errors
        newTsvWriter.flush();
        if (newTsvWriter.checkError()) {
            throw new BridgeExporterException("TSV writer has unknown error");
        }
        newTsvWriter.close();

        // filter on line count
        if (lineCount == 0) {
            // delete files with no lines--they're useless anyway
            newTsvFile.delete();
            throw new BridgeExporterException("TSV writer wrote zero lines");
        }

        // get the table, so we can figure out where to upload the file handle to
        TableEntity table;
        try {
            table = synapseHelper.getTableWithRetry(synapseTableId);
        } catch (SynapseException ex) {
            throw new BridgeExporterException("Error fetching table " + synapseTableId + " for file " +
                    newTsvFile.getAbsolutePath() + ": " + ex.getMessage(), ex);
        }
        String projectId = table.getParentId();

        long linesProcessed = synapseHelper.uploadTsvFileToTable(projectId, synapseTableId, newTsvFile);
        if (linesProcessed != lineCount) {
            throw new BridgeExporterException("Wrong number of lines processed importing to table " + synapseTableId +
                    ", expected=" + lineCount + ", actual=" + linesProcessed);
        }

        // We've successfully uploaded the file. We can delete the file now.
        newTsvFile.delete();
    }
}
