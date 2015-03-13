package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

public class BridgeExporterOld {
    private static final String S3_BUCKET_UPLOAD = "org-sagebridge-upload-dwaynejeng";
    private static final String S3_BUCKET_EXPORT = "org-sagebridge-export-dwaynejeng";

    public static void main(String[] args) throws InterruptedException, IOException {
        new BridgeExporterOld().run();
        System.exit(0);
    }

    private DynamoDB ddbClient;
    private DynamoDbHelper ddbHelper;
    private TransferManager s3TransferManager;
    private LocalDate todaysDate;
    private String todaysDateString;
    private File tempDir;

    public void run() throws InterruptedException, IOException {
        init();
        exportUploads();
        exportTables();
        cleanUp();
    }

    private void init() throws IOException {
        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        ddbHelper = new DynamoDbHelper(ddbClient);

        // S3 client - move to Spring
        s3TransferManager = new TransferManager();

        // TODO: If we globalize Bridge, we'll need to make this timezone configurable.
        //todaysDate = LocalDate.now(DateTimeZone.forID("America/Los_Angeles"));
        todaysDate = LocalDate.parse("2014-12-15", ISODateTimeFormat.date());
        todaysDateString = todaysDate.toString(ISODateTimeFormat.date());

        // set up temp dir we want to write to
        String tempDirName = String.format("%s/Bridge-Exporter-%s", System.getProperty("java.io.tmpdir"),
                UUID.randomUUID().toString());
        tempDir = new File(tempDirName);
        if (!tempDir.mkdirs()) {
            throw new IOException(String.format("failed to create temp dir %s", tempDirName));
        }
        System.out.format("Tempdir: %s", tempDirName);
        System.out.println();
    }

    private void exportUploads() throws InterruptedException, IOException {
        // query DDB
        ItemCollection<QueryOutcome> ddbResultColl = ddbHelper.getUploadsForDate(todaysDate);

        // download files
        List<Download> downloadList = new ArrayList<>();
        Set<String> studySet = new HashSet<>();
        for (Item oneItem : ddbResultColl) {
            String s3Key = oneItem.getString("objectId");
            String originalFilename = oneItem.getString("name");
            String fileBaseName = Files.getNameWithoutExtension(originalFilename);
            String fileExtension = Files.getFileExtension(originalFilename);
            String uploadId = oneItem.getString("uploadId");
            String newFilename;
            if (Strings.isNullOrEmpty(fileExtension)) {
                newFilename = String.format("%s.%s", fileBaseName, uploadId);
            } else {
                newFilename = String.format("%s.%s.%s", fileBaseName, uploadId, fileExtension);
            }

            String healthCode = oneItem.getString("healthCode");
            String studyId = ddbHelper.getStudyForHealthcode(healthCode);
            studySet.add(studyId);

            File studyDir = new File(tempDir, studyId);
            if (!studyDir.exists()) {
                if (!studyDir.mkdir()) {
                    throw new IOException(String.format("failed to create study dir for %s", studyId));
                }
            }
            File localFile = new File(studyDir, newFilename);
            Download s3Download = s3TransferManager.download(S3_BUCKET_UPLOAD, s3Key,localFile);
            downloadList.add(s3Download);
        }

        // Files are downloaded asynchronously. Wait for the files to complete downloading.
        for (Download oneDownload : downloadList) {
            oneDownload.waitForCompletion();
        }

        // Zip files per study
        List<Upload> uploadList = new ArrayList<>();
        for (String oneStudyId : studySet) {
            File studyDir = new File(tempDir, oneStudyId);
            File[] studyFileArr = studyDir.listFiles();

            String zipFileName = String.format("%s-%s.zip", oneStudyId, todaysDateString);
            File studyZipFile = new File(tempDir, zipFileName);
            String base64EncodedMd5;
            try (HashingOutputStream hashOut = new HashingOutputStream(Hashing.md5(),
                    new FileOutputStream(studyZipFile));
                    ZipOutputStream zipOut = new ZipOutputStream(hashOut)) {
                for (File oneStudyFile : studyFileArr) {
                    zipOut.putNextEntry(new ZipEntry(oneStudyFile.getName()));
                    Files.copy(oneStudyFile, zipOut);
                    zipOut.closeEntry();
                }
                zipOut.finish();

                byte[] md5Bytes = hashOut.hash().asBytes();
                base64EncodedMd5 = BaseEncoding.base64().encode(md5Bytes);
            }

            // upload zip files to S3
            String exportObjectId = UUID.randomUUID().toString();
            Upload s3Upload = s3TransferManager.upload(S3_BUCKET_EXPORT, exportObjectId,  studyZipFile);
            uploadList.add(s3Upload);

            // write to exports table
            Table exportsTable = ddbClient.getTable("local-DwayneJeng-Exports");
            Item exportItem = new Item().withPrimaryKey("s3ObjectId", exportObjectId)
                    .withLong("contentLength", studyZipFile.length()).withString("contentMd5", base64EncodedMd5)
                    .withString("contentType", "application/zip").withString("filename", zipFileName)
                    .withString("studyId", oneStudyId).withString("uploadDate", todaysDateString);
            exportsTable.putItem(exportItem);
        }

        for (Upload oneUpload : uploadList) {
            oneUpload.waitForCompletion();
        }
    }

    private void exportTables() {
    }

    private void cleanUp() {
        ddbClient.shutdown();
        s3TransferManager.shutdownNow();
    }
}
