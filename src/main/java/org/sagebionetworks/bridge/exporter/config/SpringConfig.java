package org.sagebionetworks.bridge.exporter.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterSqsCallback;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.sagebionetworks.bridge.sqs.SqsHelper;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.exporter")
@Configuration
public class SpringConfig {
    public static final String CONFIG_KEY_ATTACHMENT_S3_BUCKET = "attachment.bucket";

    private static final String CONFIG_FILE = "BridgeExporter.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    @Bean
    public Config bridgeConfig() {
        String defaultConfig = getClass().getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(defaultConfigPath, localConfigPath);
            } else {
                return new PropertiesConfig(defaultConfigPath);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean(name = "ddbPrefix")
    public String ddbPrefix() {
        Config config = bridgeConfig();
        String envName = config.getEnvironment().name().toLowerCase();
        String userName = config.getUser();
        return envName + '-' + userName + '-';
    }

    @Bean(name = "ddbAttachmentTable")
    public Table ddbAttachmentTable() {
        return ddbClient().getTable(ddbPrefix() + "HealthDataAttachment");
    }

    @Bean(name = "ddbParticipantOptionsTable")
    public Table ddbParticipantOptionsTable() {
        return ddbClient().getTable(ddbPrefix() + "ParticipantOptions");
    }

    @Bean
    public DynamoQueryHelper ddbQueryHelper() {
        return new DynamoQueryHelper();
    }

    @Bean(name = "ddbRecordTable")
    public Table ddbRecordTable() {
        return ddbClient().getTable(ddbPrefix() + "HealthDataRecord3");
    }

    @Bean(name = "ddbRecordUploadDateIndex")
    public Index ddbRecordUploadDateIndex() {
        return ddbRecordTable().getIndex("uploadDate-index");
    }

    @Bean(name = "ddbSchemaTable")
    public Table ddbSchemaTable() {
        return ddbClient().getTable(ddbPrefix() + "UploadSchema");
    }

    @Bean(name = "ddbStudyTable")
    public Table ddbStudyTable() {
        return ddbClient().getTable(ddbPrefix() + "Study");
    }

    @Bean
    public FileHelper fileHelper() {
        return new FileHelper();
    }

    @Bean
    public S3Helper s3Helper() {
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(new AmazonS3Client());
        return s3Helper;
    }

    @Bean
    public SqsHelper sqsHelper() {
        SqsHelper sqsHelper = new SqsHelper();
        sqsHelper.setSqsClient(new AmazonSQSClient());
        return sqsHelper;
    }

    @Bean
    @Autowired
    public PollSqsWorker sqsWorker(BridgeExporterSqsCallback exporterSqsCallback) {
        Config config = bridgeConfig();

        PollSqsWorker sqsWorker = new PollSqsWorker();
        sqsWorker.setCallback(exporterSqsCallback);
        sqsWorker.setQueueUrl(config.get("exporter.request.sqs.queue.url"));
        sqsWorker.setSleepTimeMillis(config.getInt("exporter.request.sqs.sleep.time.millis"));
        sqsWorker.setSqsHelper(sqsHelper());
        return sqsWorker;
    }

    @Bean
    public SynapseClient synapseClient() {
        Config config = bridgeConfig();

        SynapseClient synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(config.get("synapse.user"));
        synapseClient.setApiKey(config.get("synapse.api.key"));
        return synapseClient;
    }

    @Bean(name = "workerExecutorService")
    public ExecutorService workerExecutorService() {
        return Executors.newFixedThreadPool(bridgeConfig().getInt("threadpool.worker.count"));
    }
}
