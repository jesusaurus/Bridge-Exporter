package org.sagebionetworks.bridge.worker;

/** Describes the export table task, such as processing a record or uploading the TSV. */
public enum ExportTaskType {
    /** Standard task, which includes a record ID to process and write to the TSV. */
    PROCESS_RECORD,

    /**
     * Processes a record from an iOS survey. This record already has its data extracted to JSON, although metadata is
     * still present in the original DDB record.
     */
    PROCESS_IOS_SURVEY,

    /**
     * Signals to the worker that the end of the record stream has been reached. For iOS survey workers, this ends
     * processing. For non-iOS-survey workers, this also uploads the TSV to Synapse.
     */
    END_OF_STREAM,
}
