package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.worker.ExportTask;
import org.sagebionetworks.bridge.worker.ExportTaskType;
import org.sagebionetworks.bridge.worker.ExportWorkerManager;

public class BridgeExporter {
    private static final Joiner SCHEMAS_NOT_FOUND_JOINER = Joiner.on(", ");

    // Number of records before the script stops processing records. This is used for testing. To make this unlimited,
    // set it to -1.
    private static final int RECORD_LIMIT = 30;

    // Script should report progress after this many records, so users tailing the logs can see that it's still
    // making progress
    private static final int PROGRESS_REPORT_PERIOD = 10;

    public static void main(String[] args) throws IOException {
        try {
            BridgeExporter bridgeExporter = new BridgeExporter();
            bridgeExporter.setDate(args[0]);
            bridgeExporter.run();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
        } finally {
            System.exit(0);
        }
    }

    private final Map<String, Integer> counterMap = new HashMap<>();
    private final Map<String, Set<String>> setCounterMap = new HashMap<>();

    private DynamoDB ddbClient;
    private ExportWorkerManager manager;
    private UploadSchemaHelper schemaHelper;
    private String uploadDateString;

    public void run() throws IOException, SynapseException {
        System.out.println("[METRICS] Starting Bridge Exporter for date " + uploadDateString);

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            System.out.println("[METRICS] Starting initialization: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            init();
            System.out.println("[METRICS] Initialization done: " + BridgeExporterUtil.getCurrentLocalTimestamp());

            downloadHealthDataRecords();
        } finally {
            stopwatch.stop();
            System.out.println("[METRICS] Bridge Exporter done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            System.out.println("[METRICS] Total time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }
    }

    public void setDate(String dateString) {
        // validate date
        LocalDate.parse(dateString);

        this.uploadDateString = dateString;
    }

    private void init() throws IOException, SynapseException {
        LocalDate todaysDate = LocalDate.now(BridgeExporterUtil.LOCAL_TIME_ZONE);
        String todaysDateString = todaysDate.toString(ISODateTimeFormat.date());

        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());

        // S3 client - move to Spring
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // because of a bug in the Java client, we need to properly log in to upload file handles
        // see https://sagebionetworks.jira.com/browse/PLFM-3310
        synapseClient.login(config.getUsername(), config.getPassword());

        // synapse helper
        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setBridgeExporterConfig(config);
        synapseHelper.setS3Helper(s3Helper);
        synapseHelper.setSynapseClient(synapseClient);

        // schema Helper
        schemaHelper = new UploadSchemaHelper();
        schemaHelper.setDdbClient(ddbClient);
        schemaHelper.init();

        // export worker manager
        manager = new ExportWorkerManager();
        manager.setBridgeExporterConfig(config);
        manager.setDdbClient(ddbClient);
        manager.setS3Helper(s3Helper);
        manager.setSchemaHelper(schemaHelper);
        manager.setSynapseHelper(synapseHelper);
        manager.setTodaysDateString(todaysDateString);
        manager.init();
    }

    private void downloadHealthDataRecords() {
        Stopwatch progressStopwatch = Stopwatch.createStarted();

        // get key objects by querying uploadDate index
        Table recordTable = ddbClient.getTable("prod-heroku-HealthDataRecord3");
        Index recordTableUploadDateIndex = recordTable.getIndex("uploadDate-index");
        Iterable<Item> recordKeyIter = recordTableUploadDateIndex.query("uploadDate", uploadDateString);

        // re-query table to get values
        for (Item oneRecordKey : recordKeyIter) {
            // running count of records
            int numTotal = incrementCounter("numTotal");
            if (numTotal % PROGRESS_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num records so far: " + numTotal + " in "
                        + progressStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
            if (RECORD_LIMIT > 0 && numTotal > RECORD_LIMIT) {
                break;
            }

            String recordId = oneRecordKey.getString("id");
            try {
                // re-query health data records to get values
                Item oneRecord;
                try {
                    oneRecord = recordTable.getItem("id", recordId);
                } catch (AmazonClientException ex) {
                    System.out.println("[ERROR] Exception querying record for ID " + recordId + ": " + ex.getMessage());
                    continue;
                }
                if (oneRecord == null) {
                    System.out.println("[ERROR] No record for ID " + recordId);
                    continue;
                }

                // process/filter by user sharing scope
                String userSharingScope = oneRecord.getString("userSharingScope");
                if (Strings.isNullOrEmpty(userSharingScope) || userSharingScope.equalsIgnoreCase("no_sharing")) {
                    // must not be exported
                    incrementCounter("numNotShared");
                    continue;
                } else if (userSharingScope.equalsIgnoreCase("sponsors_and_partners")) {
                    incrementCounter("numSharingBroadly");
                } else if (userSharingScope.equalsIgnoreCase("all_qualified_researchers")) {
                    incrementCounter("numSharingSparsely");
                } else {
                    System.out.println("[ERROR] Unknown sharing scope: " + userSharingScope);
                    continue;
                }

                // basic record data
                String studyId = oneRecord.getString("studyId");
                String schemaId = oneRecord.getString("schemaId");
                int schemaRev = oneRecord.getInt("schemaRevision");
                UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

                String healthCode = oneRecord.getString("healthCode");
                incrementSetCounter("uniqueHealthCodes[" + studyId + "]", healthCode);

                // Multiplex and add the right tasks to the manager. Since Export Tasks are immutable, it's safe to
                // shared the same export task.
                ExportTask task = new ExportTask(ExportTaskType.PROCESS_RECORD, oneRecord, null);
                if ("ios-survey".equals(schemaId)) {
                    try {
                        manager.addIosSurveyExportTask(studyId, task);
                    } catch (ExportWorkerException ex) {
                        System.out.println("[ERROR] Error queueing survey task for record " + recordId + " in study "
                                + studyId + ": " + ex.getMessage());
                    }
                } else {
                    try {
                        manager.addHealthDataExportTask(schemaKey, task);
                    } catch (SchemaNotFoundException ex) {
                        System.out.println("[ERROR] Schema not found for record " + recordId + " schema "
                                + schemaKey.toString() + ": " + ex.getMessage());
                    }
                }
                try {
                    manager.addAppVersionExportTask(studyId, task);
                } catch (ExportWorkerException ex) {
                    System.out.println("[ERROR] Error queueing app version task for record " + recordId + " in study "
                            + studyId + ": " + ex.getMessage());
                }
            } catch (RuntimeException ex) {
                System.out.println("[ERROR] Unknown error processing record " + recordId + ": " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        // signal the worker manager that we've reached the end of the stream
        manager.endOfStream();

        for (Map.Entry<String, Integer> oneCounter : counterMap.entrySet()) {
            System.out.println("[METRICS] " + oneCounter.getKey() + ": " + oneCounter.getValue());
        }
        for (Map.Entry<String, Set<String>> oneSetCounter : setCounterMap.entrySet()) {
            System.out.println("[METRICS] " + oneSetCounter.getKey() + ": " + oneSetCounter.getValue().size());
        }

        Set<String> schemasNotFound = new TreeSet<>();
        schemasNotFound.addAll(manager.getSchemasNotFound());
        schemasNotFound.addAll(schemaHelper.getSchemasNotFound());
        if (!schemasNotFound.isEmpty()) {
            System.out.println("[METRICS] The following schemas were referenced but not found: "
                    + SCHEMAS_NOT_FOUND_JOINER.join(schemasNotFound));
        }
    }

    private int incrementCounter(String name) {
        Integer oldValue = counterMap.get(name);
        int newValue;
        if (oldValue == null) {
            newValue = 1;
        } else {
            newValue = oldValue + 1;
        }

        counterMap.put(name, newValue);
        return newValue;
    }

    // Only increments the counter if the value hasn't already been used. Used for things like counting unique health
    // codes.
    private void incrementSetCounter(String name, String value) {
        Set<String> set = setCounterMap.get(name);
        if (set == null) {
            set = new HashSet<>();
            setCounterMap.put(name, set);
        }
        set.add(value);
    }
}
