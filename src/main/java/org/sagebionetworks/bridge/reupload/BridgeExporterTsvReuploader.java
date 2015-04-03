package org.sagebionetworks.bridge.reupload;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.UploadToTableResult;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * The Bridge Exporter logs will spit out entries that look like
 *
 * [re-upload-tsv] file /tmp/cardiovascular-satisfied-v1.6348225244877621202.tsv syn3469956
 *
 * or
 *
 * [re-upload-tsv] filehandle 2439961 syn3474928
 *
 * If those lines are grepped out and fed into a file as input to this class, this class will attempt to re-upload
 * those TSVs.
 */
public class BridgeExporterTsvReuploader {
    private static final int ASYNC_UPLOAD_TIMEOUT_SECONDS = 300;

    // TODO: Global vars are bad. Make these non-static and object oriented.
    private static SynapseHelper synapseHelper;

    public static void main(String[] args) {
        try {
            init();
            processReuploadLog(args[0]);
        } catch (Throwable t) {
            // shift err into out, in case we forget to do 2&>1
            t.printStackTrace(System.out);
        } finally {
            System.exit(0);
        }
    }

    private static void init() throws IOException, SynapseException {
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

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

    private static void processReuploadLog(String filename) throws IOException {
        // read file
        File reuploadLogFile = new File(filename);
        List<String> reuploadLogLineList = Files.readLines(reuploadLogFile, Charsets.UTF_8);

        // main loop
        for (String oneLine : reuploadLogLineList) {
            try {
                String[] tokens = oneLine.split("\\s+");
                if (tokens.length != 4) {
                    System.out.println("[ERROR] Wrong number of tokens: " + oneLine);
                    continue;
                }
                String header = tokens[0];
                String type = tokens[1];
                String id = tokens[2];
                String tableId = tokens[3];

                // header is always "[re-upload-tsv]"
                if (!"[re-upload-tsv]".equals(header)) {
                    System.out.println("[ERROR] Unknown header " + header);
                    continue;
                }

                // type determines if the id is a file path or a Synapse file handle ID
                if ("file".equals(type)) {
                    processFile(id, tableId);
                } else if ("filehandle".equals(type)) {
                    processFileHandle(id, tableId);
                } else {
                    System.out.println("[ERROR] Unknown type " + type);
                    continue;
                }

                System.out.println("[STATUS] Processed: " + oneLine);
            } catch (ExportWorkerException ex) {
                System.out.println("[ERROR] Error reuploading for line: " + oneLine + ": " + ex.getMessage());
            } catch (RuntimeException ex) {
                System.out.println("[ERROR] RuntimeException reuploading for line: " + oneLine + ": " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        System.out.println("Done running re-uploader.");
    }

    // TODO: This is copy-pasted from SynapseExportWorker. Un-spaghetti this.
    private static void processFile(String filePath, String tableId) throws ExportWorkerException {
        File file = new File(filePath);

        // get the table, so we can figure out where to upload the file handle to
        TableEntity table;
        try {
            table = synapseHelper.getTableWithRetry(tableId);
        } catch (SynapseException ex) {
            throw new ExportWorkerException("Error fetching table " + tableId + " for file " + filePath + ": " +
                    ex.getMessage(), ex);
        }
        String projectId = table.getParentId();

        // upload file to synapse as a file handle
        FileHandle tableFileHandle;
        try {
            tableFileHandle = synapseHelper.createFileHandleWithRetry(file, "text/tab-separated-values", projectId);
        } catch (IOException | SynapseException ex) {
            throw new ExportWorkerException("Error uploading file " + filePath + " to table " + tableId + ": "
                    + ex.getMessage(), ex);
        }
        String fileHandleId = tableFileHandle.getId();
        System.out.println("[STATUS] Uploaded file " + filePath + " to file handle " + fileHandleId);

        processFileHandle(fileHandleId, tableId);
    }

    // TODO: This is copy-pasted from SynapseExportWorker. Un-spaghetti this.
    private static void processFileHandle(String fileHandleId, String tableId) throws ExportWorkerException {
        // start tsv import
        CsvTableDescriptor tableDesc = new CsvTableDescriptor();
        tableDesc.setIsFirstLineHeader(true);
        tableDesc.setSeparator("\t");

        String jobToken;
        try {
            jobToken = synapseHelper.uploadTsvStartWithRetry(tableId, fileHandleId, tableDesc);
        } catch (SynapseException ex) {
            throw new ExportWorkerException("Error starting async import of file handle " + fileHandleId + " to table "
                    + tableId + ": " + ex.getMessage(), ex);
        }

        // poll asyncGet until success or timeout
        boolean success = false;
        for (int sec = 0; sec < ASYNC_UPLOAD_TIMEOUT_SECONDS; sec++) {
            // sleep for 1 sec
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // noop
            }

            // poll
            try {
                UploadToTableResult uploadResult = synapseHelper.getUploadTsvStatus(jobToken, tableId);
                if (uploadResult == null) {
                    // Result not ready. Sleep some more.
                    continue;
                }

                success = true;
                break;
            } catch (SynapseResultNotReadyException ex) {
                // results not ready, sleep some more
            } catch (SynapseException ex) {
                throw new ExportWorkerException("Error polling job status of importing file handle " + fileHandleId
                        + " to table " + tableId + ": " + ex.getMessage(), ex);
            }
        }

        if (!success) {
            throw new ExportWorkerException("Timed out uploading file handle " + fileHandleId + " to table "
                    + tableId);
        }
    }
}
