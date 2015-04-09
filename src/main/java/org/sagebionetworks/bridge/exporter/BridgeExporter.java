package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.worker.ExportTask;
import org.sagebionetworks.bridge.worker.ExportWorkerManager;

public class BridgeExporter {
    private static final Joiner SCHEMAS_NOT_FOUND_JOINER = Joiner.on(", ");
    private static final TypeReference<List<UploadSchemaKey>> TYPE_REF_SCHEMA_KEY_LIST =
            new TypeReference<List<UploadSchemaKey>>() {
            };

    // Number of records before the script stops processing records. This is used for testing. To make this unlimited,
    // set it to -1.
    private static final int RECORD_LIMIT = -1;

    // Script should report progress after this many records, so users tailing the logs can see that it's still
    // making progress
    private static final int PROGRESS_REPORT_PERIOD = 2500;

    public static void main(String[] args) throws IOException {
        try {
            System.out.println("[METRICS] Starting Bridge Exporter for args " + Joiner.on(' ').join(args));

            BridgeExporter bridgeExporter = new BridgeExporter();
            if ("redrive-tables".equals(args[0])) {
                // format: BridgeExporter redrive-tables [table list file] yyyy-mm-dd
                bridgeExporter.setDate(args[2]);
                bridgeExporter.setMode(ExportMode.REDRIVE_TABLES);
                bridgeExporter.setRedriveTableKeySet(extractRedriveTableKeySet(args[1]));
            } else if ("redrive-records".equals(args[0])) {
                // format: BridgeExporter redrive-records [record list file]
                // Note that redrive-records doesn't need an upload date, since it doesn't talk to the uploadDate index
                bridgeExporter.setMode(ExportMode.REDRIVE_RECORDS);
                bridgeExporter.setRecordIdFilename(args[1]);
            } else {
                // format: BridgeExporter yyyy-mm-dd
                bridgeExporter.setDate(args[0]);
                bridgeExporter.setMode(ExportMode.EXPORT);
            }
            bridgeExporter.run();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
        } finally {
            System.exit(0);
        }
    }

    private static Set<UploadSchemaKey> extractRedriveTableKeySet(String filename) throws IOException {
        List<UploadSchemaKey> keyList = BridgeExporterUtil.JSON_MAPPER.readValue(new File(filename),
                TYPE_REF_SCHEMA_KEY_LIST);
        return ImmutableSet.copyOf(keyList);
    }

    private final Map<String, Integer> counterMap = new TreeMap<>();
    private final Map<String, Set<String>> setCounterMap = new TreeMap<>();

    // Configured externally
    private String uploadDateString;
    private ExportMode mode;
    private String recordIdFilename;
    private Set<UploadSchemaKey> redriveTableKeySet;

    // Internal state
    private ExportWorkerManager manager;
    private RecordIdSource recordIdSource;
    private UploadSchemaHelper schemaHelper;
    private Table recordTable;

    public void setDate(String dateString) {
        // validate date
        LocalDate.parse(dateString);

        this.uploadDateString = dateString;
    }

    public void setMode(ExportMode mode) {
        this.mode = mode;
    }

    public void setRecordIdFilename(String recordIdFilename) {
        this.recordIdFilename = recordIdFilename;
    }

    public void setRedriveTableKeySet(Set<UploadSchemaKey> redriveTableKeySet) {
        this.redriveTableKeySet = ImmutableSet.copyOf(redriveTableKeySet);
    }

    public void run() throws BridgeExporterException, IOException, SynapseException {
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

    private void init() throws BridgeExporterException, IOException, SynapseException {
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
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        recordTable = ddbClient.getTable("prod-heroku-HealthDataRecord3");

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
        if (mode == ExportMode.REDRIVE_TABLES) {
            manager.setSchemaWhitelist(redriveTableKeySet);
        }
        manager.setSynapseHelper(synapseHelper);
        manager.setTodaysDateString(todaysDateString);
        manager.init();

        // record ID source
        switch (mode) {
            case EXPORT:
            case REDRIVE_TABLES:
                // export and redrive tables both get their records from DDB
                DynamoRecordIdSource dynamoRecordIdSource = new DynamoRecordIdSource();
                dynamoRecordIdSource.setDdbClient(ddbClient);
                dynamoRecordIdSource.setUploadDateString(uploadDateString);
                recordIdSource = dynamoRecordIdSource;
                break;
            case REDRIVE_RECORDS:
                FileRecordIdSource fileRecordIdSource = new FileRecordIdSource();
                fileRecordIdSource.setFilename(recordIdFilename);
                recordIdSource = fileRecordIdSource;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported export mode " + mode.name());
        }
        recordIdSource.init();
    }

    private void downloadHealthDataRecords() {
        Stopwatch progressStopwatch = Stopwatch.createStarted();

        // get record IDs from record ID source and query DDB
        while (recordIdSource.hasNext()) {
            String recordId = recordIdSource.next();

            // running count of records
            int numTotal = incrementCounter("numTotal");
            if (numTotal % PROGRESS_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num records so far: " + numTotal + " in "
                        + progressStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
            if (RECORD_LIMIT > 0 && numTotal > RECORD_LIMIT) {
                break;
            }

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

                // basic record data
                String studyId = oneRecord.getString("studyId");
                String schemaId = oneRecord.getString("schemaId");
                int schemaRev = oneRecord.getInt("schemaRevision");
                UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

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

                String healthCode = oneRecord.getString("healthCode");
                incrementSetCounter("uniqueHealthCodes[" + studyId + "]", healthCode);

                // Multiplex and add the right tasks to the manager. Since Export Tasks are immutable, it's safe to
                // shared the same export task.
                ExportTask task = new ExportTask(schemaKey, oneRecord, null);
                if ("ios-survey".equals(schemaId)) {
                    try {
                        manager.addIosSurveyExportTask(studyId, task);
                    } catch (BridgeExporterException ex) {
                        System.out.println("[ERROR] Error queueing survey task for record " + recordId + " in study "
                                + studyId + ": " + ex.getMessage());
                    }
                } else {
                    try {
                        manager.addHealthDataExportTask(schemaKey, task);
                    } catch (SchemaNotFoundException ex) {
                        System.out.println("[ERROR] Schema not found for record " + recordId + " schema "
                                + schemaKey.toString() + ": " + ex.getMessage());
                    } catch (BridgeExporterException ex) {
                        System.out.println("[ERROR] Error queueing app version task for record " + recordId + " in study "
                                + studyId + ": " + ex.getMessage());
                    }
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
