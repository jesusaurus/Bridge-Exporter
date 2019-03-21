package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterTsvException;

/**
 * Helper class that keeps track of a TSV file, the writer that writes to the file, and a method for tracking and
 * incrementing TSV line counts.
 */
public class TsvInfo {
    private static final Logger LOG = LoggerFactory.getLogger(TsvInfo.class);

    private final List<String> columnNameList;
    private final File file;
    private final CSVWriter tsvWriter;
    private final Throwable initError;
    private final List<String> recordIds = new ArrayList<>();

    private int lineCount = 0;

    /**
     * TSV info constructor.
     *
     * @param columnNameList
     *         list of column names in this TSV
     * @param file
     *         TSV file
     * @param writer
     *         writer for the TSV file
     */
    public TsvInfo(List<String> columnNameList, File file, Writer writer) {
        this.columnNameList = columnNameList;
        this.file = file;
        this.initError = null;

        // Set CsvWriter with tab separator character.
        this.tsvWriter = new CSVWriter(writer, '\t');

        // Write headers. The new String[0] looks weird, but according to official Oracle javadoc, this is how you use
        // toArray().
        tsvWriter.writeNext(columnNameList.toArray(new String[0]));
    }

    /**
     * Constructs a TsvInfo that failed to initialize. Used to represent, for example, TSVs where creating or updating
     * the Synapse table failed, or where the schema changes dictate an impossible table update. This wraps an internal
     * exception, so that calls further down the chain can handle the error accordingly.
     */
    public TsvInfo(Throwable t) {
        this.columnNameList = null;
        this.file = null;
        this.tsvWriter = null;
        this.initError = t;
    }

    /** Checks if the TSV is properly initialized. Throws a BridgeExporterException if it isn't. */
    public void checkInitAndThrow() throws BridgeExporterException {
        if (initError != null) {
            throw new BridgeExporterTsvException("TSV was not successfully initialized: " + initError.getMessage(),
                    initError);
        }
    }

    /** Flushes and closes the writer. This also checks the writer for errors and will throw if there are errors. */
    public void flushAndCloseWriter() throws BridgeExporterException {
        checkInitAndThrow();

        // Error handling code here is a bit of a mess. Internally, CSVWriter creates a PrintWriter, which doesn't
        // throw, but exposes checkError() check for errors. However, CSVWriter declares that flush() throws, even
        // though internally it doesn't, and then in close(), it calls close on both the PrintWriter and the internal
        // Writer, one of which closes, and the other does not. Because we need to test with the actual CSVWriter
        // implementation to make sure all the libraries are doing what we expect, this makes testing error handling
        // code difficult at best. As such, none of the error handling code is exercised by tests. Fortunately, we log
        // and rethrow very thoroughly, so if an error occurs, we shouldn't have any problems.
        try {
            tsvWriter.flush();
            if (tsvWriter.checkError()) {
                LOG.error("TSV writer has unknown error");
                throw new BridgeExporterException("TSV writer has unknown error");
            }
        } catch (IOException ex) {
            LOG.error("Error flushing TSV writer: " + ex.getMessage(), ex);
            throw new BridgeExporterException("Error flushing TSV writer: " + ex.getMessage(), ex);
        } finally {
            try {
                tsvWriter.close();
            } catch (IOException ex) {
                LOG.error("Error closing TSV writer: " + ex.getMessage(), ex);
                //noinspection ThrowFromFinallyBlock
                throw new BridgeExporterException("Error closing TSV writer: " + ex.getMessage(), ex);
            }
        }
    }

    /** TSV file. */
    public File getFile() {
        return file;
    }

    /** Number of lines written to TSV file. */
    public int getLineCount() {
        return lineCount;
    }

    /**
     * helper method to add a record id into the list
     */
    public void addRecordId(String recordId) {
        this.recordIds.add(recordId);
    }

    public List<String> getRecordIds() {
        return ImmutableList.copyOf(this.recordIds);
    }

    /**
     * Writes the row to the TSV writer and increments the line count. Automatically appends a newline. If there are
     * missing or extra values, this method silently ignores them, for backwards compatibility with older formats.
     *
     * @param rowValueMap
     *         Map representing the row. Keys are column names, values are column values.
     * @throws BridgeExporterException
     *         if the TSV info was not properly initialized
     */
    public synchronized void writeRow(Map<String, String> rowValueMap) throws BridgeExporterException {
        checkInitAndThrow();

        // Using the columnNameList, go through the row values in order and flatten them into an array.
        int numColumns = columnNameList.size();
        String[] rowValueArray = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            rowValueArray[i] = rowValueMap.get(columnNameList.get(i));
        }

        tsvWriter.writeNext(rowValueArray);
        lineCount++;
    }
}
