package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

import org.sagebionetworks.bridge.s3.S3Helper;

public class BridgeExporter {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");
    private static final String S3_BUCKET_ATTACHMENTS = "org-sagebridge-attachment-prod";

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

    private final Map<String, Integer> counterMap = new HashMap<>();
    private final Map<String, Set<String>> setCounterMap = new HashMap<>();

    private DynamoDB ddbClient;
    //private DynamoDbHelper ddbHelper;
    //private TransferManager s3TransferManager;
    private LocalDate date;
    private String dateString;
    private UploadSchemaHelper schemaHelper;
    private S3Helper s3Helper;
    private File tempDir;

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
        AmazonS3Client s3Client = new AmazonS3Client();
        s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // set up temp dir we want to write to
        String tempDirName = System.getProperty("user.home") + "/Bridge-Exporter/" + dateString;
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
        schemaHelper.setDateString(dateString);
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
        Set<String> schemasNotFound = new TreeSet<>();
        for (Item oneRecordKey : recordKeyIter) {
            int numTotal = incrementCounter("numTotal");
            if (numTotal % 100 == 0) {
                System.out.println("Saw " + numTotal + " files so far...");
            }
            if (numTotal > 100) {
                break;
            }

            Item oneRecord = recordTable.getItem("id", oneRecordKey.get("id"));

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
                System.out.println("Unknown sharing scope: " + userSharingScope);
            }

            String recordId = oneRecord.getString("id");
            String studyId = oneRecord.getString("studyId");
            String schemaId = oneRecord.getString("schemaId");
            int schemaRev = oneRecord.getInt("schemaRevision");

            String healthCode = oneRecord.getString("healthCode");
            incrementSetCounter("uniqueHealthCodes[" + studyId + "]", healthCode);

            String metadataString = oneRecord.getString("metadata");
            JsonNode metadataJson = !Strings.isNullOrEmpty(metadataString) ? JSON_MAPPER.readTree(metadataString)
                    : null;
            JsonNode taskRunNode = metadataJson != null ? metadataJson.get("taskRun") : null;
            String taskRunId = taskRunNode != null ? taskRunNode.textValue() : null;

            JsonNode dataJson;
            if ("ios-survey".equals(schemaId)) {
                incrementCounter("numSurveys");

                // TODO: move survey translation layer to server-side
                JsonNode oldDataJson = JSON_MAPPER.readTree(oneRecord.getString("data"));
                JsonNode itemNode = oldDataJson.get("item");
                if (itemNode == null) {
                    System.out.println("Survey with no item for record ID " + recordId);
                    continue;
                }
                String item = itemNode.textValue();
                if (Strings.isNullOrEmpty(item)) {
                    System.out.println("Survey with null or empty item for record ID " + recordId);
                    continue;
                }
                schemaId = item;

                // surveys default to rev 1 until this code is moved to server side
                schemaRev = 1;

                // download answers from S3 attachments
                JsonNode answerLinkNode = oldDataJson.get("answers");
                if (answerLinkNode == null) {
                    System.out.println("Survey with no answer link for record ID " + recordId);
                    continue;
                }
                String answerLink = answerLinkNode.textValue();
                String answerText = s3Helper.readS3FileAsString(S3_BUCKET_ATTACHMENTS, answerLink);

                JsonNode answerArrayNode = JSON_MAPPER.readTree(answerText);
                ObjectNode convertedSurveyNode = JSON_MAPPER.createObjectNode();
                if (answerArrayNode == null) {
                    System.out.println("Survey with no answers for record ID " + recordId);
                    continue;
                }

                // copy fields to "non-survey" format
                int numAnswers = answerArrayNode.size();
                for (int i = 0; i < numAnswers; i++) {
                    JsonNode oneAnswerNode = answerArrayNode.get(i);

                    // question name ("item")
                    JsonNode answerItemNode = oneAnswerNode.get("item");
                    if (answerItemNode == null) {
                        System.out.println("Survey record ID " + recordId + " answer " + i
                                + " has no question name (item)");
                        continue;
                    }
                    String answerItem = answerItemNode.asText();
                    if (Strings.isNullOrEmpty(answerItem)) {
                        System.out.println("Survey record ID " + recordId + " answer " + i
                                + " has null or empty question name (item)");
                        continue;
                    }

                    // question type
                    JsonNode questionTypeNameNode = oneAnswerNode.get("questionTypeName");
                    if (questionTypeNameNode == null) {
                        // fall back to questionType
                        questionTypeNameNode = oneAnswerNode.get("questionType");
                    }
                    if (questionTypeNameNode == null) {
                        System.out.println("Survey record ID " + recordId + " answer " + i + " has no question type");
                        continue;
                    }
                    String questionTypeName = questionTypeNameNode.asText();
                    if (Strings.isNullOrEmpty(questionTypeName)) {
                        System.out.println("Survey record ID " + recordId + " answer " + i
                                + " has null or empty question type");
                        continue;
                    }

                    // answer
                    JsonNode answerAnswerNode = null;
                    switch (questionTypeName) {
                        case "Boolean":
                            answerAnswerNode = oneAnswerNode.get("booleanAnswer");
                            break;
                        case "Date":
                            answerAnswerNode = oneAnswerNode.get("dateAnswer");
                            break;
                        case "Decimal":
                        case "Integer":
                            answerAnswerNode = oneAnswerNode.get("numericAnswer");
                            break;
                        case "MultipleChoice":
                        case "SingleChoice":
                            answerAnswerNode = oneAnswerNode.get("choiceAnswers");
                            break;
                        case "None":
                        case "Scale":
                            // yes, None really gets the answer from scaleAnswer
                            answerAnswerNode = oneAnswerNode.get("scaleAnswer");
                            break;
                        case "Text":
                            answerAnswerNode = oneAnswerNode.get("textAnswer");
                            break;
                        case "TimeInterval":
                            answerAnswerNode = oneAnswerNode.get("intervalAnswer");
                            break;
                        default:
                            System.out.println("Survey record ID " + recordId + " answer " + i
                                    + " has unknown question type " + questionTypeName);
                            break;
                    }
                    convertedSurveyNode.set(answerItem, answerAnswerNode);

                    // if there's a unit, add it as well
                    JsonNode unitNode = oneAnswerNode.get("unit");
                    if (unitNode != null && !unitNode.isNull()) {
                        convertedSurveyNode.set(answerItem + "_unit", unitNode);
                    }

                    // TODO: attachment types
                }

                dataJson = convertedSurveyNode;
            } else {
                incrementCounter("numNonSurveys");
                dataJson = JSON_MAPPER.readTree(oneRecord.getString("data"));
            }

            UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);
            Item schema = schemaHelper.getSchema(schemaKey);
            if (schema == null) {
                // No schema. Skip.
                String fullSchemaId = studyId + ":" + schemaId + "-v" + schemaRev;
                System.out.println("Schema " + fullSchemaId + " not found for record " + recordId);
                schemasNotFound.add(fullSchemaId);
                continue;
            }

            PrintWriter writer = schemaHelper.getExportWriter(schemaKey);
            writer.print(recordId);
            writer.print("\t");
            writer.print(healthCode);
            writer.print("\t");
            writer.print(dateString);
            writer.print("\t");
            writer.print(new DateTime(oneRecord.getLong("createdOn")).withZone(LOCAL_TIME_ZONE)
                    .toString(ISODateTimeFormat.dateTime()));
            writer.print("\t");
            // TODO: metadata
            writer.print("(TODO: link to file)");
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

                    // TODO
                    // The following fields in the following schemas need special casing to convert from strings to
                    // attachments
                    // Breast Cancer
                    // * Daily Journal
                    //   * content_data.APHMoodLogNoteText
                    //   * DailyJournalStep103_data.content
                    // * Exercise Journal
                    //   * exercisesurvey101_data.result through exercisesurvey106_data.result

                    switch (type) {
                        case "attachment_blob":
                        case "attachment_csv":
                        case "attachment_json_blob":
                        case "attachment_json_table":
                            // TODO:
                            value = "(TODO: link to file)";
                            break;
                        case "calendar_date":
                            // sanity check
                            String dateStr = valueNode.textValue();
                            try {
                                LocalDate date = LocalDate.parse(dateStr);
                                value = dateStr;
                            } catch (RuntimeException ex) {
                                // throw out malformatted dates
                                value = null;
                            }
                            break;
                        case "timestamp":
                            if (valueNode.isTextual()) {
                                try {
                                    DateTime dateTime = DateTime.parse(valueNode.textValue())
                                            .withZone(LOCAL_TIME_ZONE);
                                    value = dateTime.toString(ISODateTimeFormat.dateTime());
                                } catch (RuntimeException ex) {
                                    // throw out malformatted timestamps
                                    value = null;
                                }
                            } else if (valueNode.isNumber()) {
                                DateTime dateTime = new DateTime(valueNode.longValue()).withZone(LOCAL_TIME_ZONE);
                                value = dateTime.toString(ISODateTimeFormat.dateTime());
                            } else {
                                // throw out malformatted timestamps
                                value = null;
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
                writer.print(value != null ? value : "");
            }
            writer.println();
        }

        for (Map.Entry<String, Integer> oneCounter : counterMap.entrySet()) {
            System.out.println(oneCounter.getKey() + ": " + oneCounter.getValue());
        }
        for (Map.Entry<String, Set<String>> oneSetCounter : setCounterMap.entrySet()) {
            System.out.println(oneSetCounter.getKey() + ": " + oneSetCounter.getValue().size());
        }
        if (!schemasNotFound.isEmpty()) {
            System.out.println("The following schemas were referenced but not found: "
                    + Joiner.on(", ").join(schemasNotFound));
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
