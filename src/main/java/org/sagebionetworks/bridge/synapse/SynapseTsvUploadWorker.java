package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.UploadToTableResult;

public class SynapseTsvUploadWorker implements Runnable {
    private static final int ASYNC_UPLOAD_TIMEOUT_SECONDS = 300;

    private int expectedLineCount;
    private String projectId;
    private SynapseClient synapseClient;
    private String tableId;
    private File tsvFile;

    public void setExpectedLineCount(int expectedLineCount) {
        this.expectedLineCount = expectedLineCount;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public void setTsvFile(File tsvFile) {
        this.tsvFile = tsvFile;
    }

    @Override
    public void run() {
        String tsvPath = tsvFile.getAbsolutePath();

        try {
            // upload file to synapse as a file handle
            FileHandle tableFileHandle;
            try {
                tableFileHandle = synapseClient.createFileHandle(tsvFile, "text/tab-separated-values", projectId);
            } catch (IOException | SynapseException ex) {
                System.out.println("Error uploading file " + tsvPath + ", for use with table " + tableId + ": "
                        + ex.getMessage());
                return;
            }
            String fileHandleId = tableFileHandle.getId();

            // We've successfully uploaded the file. We can delete the file now.
            tsvFile.delete();

            // start tsv import
            CsvTableDescriptor tableDesc = new CsvTableDescriptor();
            tableDesc.setIsFirstLineHeader(true);
            tableDesc.setSeparator("\t");

            String jobToken;
            try {
                jobToken = synapseClient.uploadCsvToTableAsyncStart(tableId, fileHandleId, null, null, tableDesc);
            } catch (SynapseException ex) {
                System.out.println("Error starting async upload of file handle " + fileHandleId + " to table "
                        + tableId + ": " + ex.getMessage());
                return;
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
                    UploadToTableResult uploadResult = synapseClient.uploadCsvToTableAsyncGet(jobToken, tableId);
                    Long linesProcessed = uploadResult.getRowsProcessed();
                    if (linesProcessed == null || linesProcessed != expectedLineCount) {
                        System.out.println("Wrong number of lines processed uploading file handle " + fileHandleId
                                + " to table " + tableId + ", expected=" + expectedLineCount + ", actual="
                                + linesProcessed);
                        return;
                    }

                    success = true;
                    break;
                } catch (SynapseResultNotReadyException ex) {
                    // results not ready, sleep some more
                } catch (SynapseException ex) {
                    System.out.println("Error polling job status of uploading file handle " + fileHandleId
                            + " to table " + tableId + ": " + ex.getMessage());
                    return;
                }
            }

            if (!success) {
                System.out.println("Timed out uploading file handle " + fileHandleId + " to table " + tableId);
            }
        } catch (Throwable t) {
            System.out.println("Unknown error uploading file " + tsvPath + " for table " + tableId + ": "
                    + t.getMessage());
            t.printStackTrace(System.out);
        }
    }
}
