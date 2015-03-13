package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

public class BridgeExporter {
    private static ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        try {
            BridgeExporter bridgeExporter = new BridgeExporter();
            bridgeExporter.setDate(args[0]);
            bridgeExporter.run();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    private DynamoDB ddbClient;
    //private DynamoDbHelper ddbHelper;
    //private TransferManager s3TransferManager;
    private LocalDate date;
    private String dateString;
    private File tempDir;
    private UploadSchemaHelper schemaHelper;

    public void run() throws IOException, ExecutionException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            init();
            downloadHealthDataRecords();
        } finally {
            if (schemaHelper != null) {
                schemaHelper.closeAllFileHandlers();
            }
        }
        stopwatch.stop();
        System.out.println("Took " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    public void setDate(String dateString) {
        this.dateString = dateString;
        this.date = LocalDate.parse(dateString);
    }

    private void init() throws IOException {
        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        //ddbHelper = new DynamoDbHelper(ddbClient);

        // S3 client - move to Spring
        //s3TransferManager = new TransferManager();

        // set up temp dir we want to write to
        String tempDirName = System.getProperty("java.io.tmpdir") + "Bridge-Exporter/" + dateString;
        tempDir = new File(tempDirName);
        if (!tempDir.exists()) {
            if (!tempDir.mkdirs()) {
                throw new IOException(String.format("failed to create temp dir %s", tempDirName));
            }
        }
        System.out.format("Tempdir: %s", tempDirName);
        System.out.println();

        Table uploadSchemaTable = ddbClient.getTable("prod-heroku-UploadSchema");
        schemaHelper = new UploadSchemaHelper();
        schemaHelper.setSchemaTable(uploadSchemaTable);
        schemaHelper.setTmpDir(tempDir);
        schemaHelper.init();
    }

    private void downloadHealthDataRecords() throws ExecutionException, IOException {
        // get key objects by querying uploadDate index
        Table recordTable = ddbClient.getTable("prod-heroku-HealthDataRecord3");
        Index recordTableUploadDateIndex = recordTable.getIndex("uploadDate-index");
        Iterable<Item> recordKeyIter = recordTableUploadDateIndex.query("uploadDate", dateString);

        // re-query table to get values
        int numNonSurveys = 0;
        int numSurveys = 0;
        int numNotShared = 0;
        Set<String> schemasNotFound = new HashSet<>();
        for (Item oneRecordKey : recordKeyIter) {
            Item oneRecord = recordTable.getItem("id", oneRecordKey.get("id"));

            String userSharingScope = oneRecord.getString("userSharingScope");
            if (Strings.isNullOrEmpty(userSharingScope) || userSharingScope.equalsIgnoreCase("no_sharing")) {
                // must not be exported
                numNotShared++;
                continue;
            }

            String schemaId = oneRecord.getString("schemaId");

            String metadataString = oneRecord.getString("metadata");
            JsonNode metadataJson = !Strings.isNullOrEmpty(metadataString) ? JSON_MAPPER.readTree(metadataString)
                    : null;
            JsonNode taskRunNode = metadataJson != null ? metadataJson.get("taskRun") : null;
            String taskRunId = taskRunNode != null ? taskRunNode.textValue() : null;

            JsonNode dataJson = JSON_MAPPER.readTree(oneRecord.getString("data"));

            if ("ios-survey".equals(schemaId)) {
                // TODO
                numSurveys++;
            } else {
                UploadSchemaKey schemaKey = new UploadSchemaKey(oneRecord.getString("studyId"),
                        oneRecord.getString("schemaId"), oneRecord.getInt("schemaRevision"));

                Item schema = schemaHelper.getSchema(schemaKey);
                if (schema == null) {
                    // No schema. Skip.
                    schemasNotFound.add(schemaKey.getStudyId() + ":" + schemaKey.getSchemaId() + "-v"
                            + schemaKey.getRev());
                    continue;
                }

                PrintWriter writer = schemaHelper.getExportWriter(schemaKey);
                writer.print(oneRecord.getString("healthCode"));
                writer.print("\t");
                writer.print(dateString);
                writer.print("\t");
                writer.print(new DateTime(oneRecord.getLong("createdOn")).toString(ISODateTimeFormat.dateTime()));
                writer.print("\t");
                writer.print(oneRecord.getString("metadata").replaceAll("\t+", " "));
                writer.print("\t");
                writer.print(userSharingScope);
                writer.print("\t");
                writer.print(taskRunId);

                JsonNode fieldDefList = JSON_MAPPER.readTree(schema.getString("fieldDefinitions"));
                for (JsonNode oneFieldDef : fieldDefList) {
                    String name = oneFieldDef.get("name").textValue();

                    String value = null;
                    if (dataJson.hasNonNull(name)) {
                        JsonNode valueNode = dataJson.get(name);

                        String type = oneFieldDef.get("type").textValue();
                        if (Strings.isNullOrEmpty(type)) {
                            type = "inline_json_blob";
                        }
                        type = type.toLowerCase();

                        switch (type) {
                            case "attachment_blob":
                            case "attachment_csv":
                            case "attachment_json_blob":
                            case "attachment_json_table":
                                // TODO:
                                value = "TODO";
                                break;
                            case "calendar_date":
                                // sanity check
                                String dateStr = valueNode.textValue();
                                try {
                                    LocalDate date = LocalDate.parse(dateStr);
                                    value = dateStr;
                                } catch (RuntimeException ex) {
                                    // fall back to verbatim JSON
                                    value = valueNode.toString();
                                }
                                break;
                            case "timestamp":
                                if (valueNode.isTextual()) {
                                    try {
                                        DateTime dateTime = DateTime.parse(valueNode.textValue());
                                        value = dateTime.toString(ISODateTimeFormat.dateTime());
                                    } catch (RuntimeException ex) {
                                        // fall back to verbatim JSON
                                        value = valueNode.toString();
                                    }
                                } else if (valueNode.isNumber()) {
                                    DateTime dateTime = new DateTime(valueNode.longValue());
                                    value = dateTime.toString(ISODateTimeFormat.dateTime());
                                } else {
                                        // fall back to verbatim JSON
                                        value = valueNode.toString();
                                }
                                break;
                            case "boolean":
                            case "float":
                            case "inline_json_blob":
                            case "int":
                            case "string":
                            default:
                                // verbatim JSON value
                                value = valueNode.toString();
                                break;
                        }

                        // replace tabs
                        if (value != null) {
                            value = value.replaceAll("\t+", " ");
                        }
                    }

                    writer.print("\t");
                    writer.print(value);
                }
                writer.println();

                numNonSurveys++;
                if (numNonSurveys >= 100) {
                    break;
                }
            }
        }

        System.out.println("Non-surveys: " + numNonSurveys);
        System.out.println("Surveys: " + numSurveys);
        System.out.println("Not shared: " + numNotShared);
        if (!schemasNotFound.isEmpty()) {
            System.out.println("The following schemas were referenced but not found: "
                    + Joiner.on(", ").join(schemasNotFound));
        }
    }
}
