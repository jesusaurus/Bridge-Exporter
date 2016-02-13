package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

/**
 * Helper class that keeps track of a TSV file, the writer that writes to the file, and a method for tracking and
 * incrementing TSV line counts.
 */
public class TsvInfo {
    private static final Joiner JOINER_COLUMN_JOINER = Joiner.on('\t').useForNull("");

    private final List<String> columnNameList;
    private final File file;
    private final PrintWriter writer;

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
    public TsvInfo(List<String> columnNameList, File file, PrintWriter writer) {
        this.columnNameList = columnNameList;
        this.file = file;
        this.writer = writer;

        writer.println(JOINER_COLUMN_JOINER.join(columnNameList));
    }

    /** Flushes and closes the writer. This also checks the writer for errors and will throw if there are errors. */
    public void flushAndCloseWriter() throws BridgeExporterException {
        writer.flush();
        if (writer.checkError()) {
            throw new BridgeExporterException("TSV writer has unknown error");
        }
        writer.close();
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
     * Writes the row to the TSV writer and increments the line count. Automatically appends a newline. If there are
     * missing or extra values, this method silently ignores them, for backwards compatibility with older formats.
     *
     * @param rowValueMap
     *         Map representing the row. Keys are column names, values are column values.
     */
    public synchronized void writeRow(Map<String, String> rowValueMap) {
        // Using the columnNameList, go through the row values in order and flatten them into a list.
        List<String> rowValueList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (String oneColumnName : columnNameList) {
            rowValueList.add(rowValueMap.get(oneColumnName));
        }

        writer.println(JOINER_COLUMN_JOINER.join(rowValueList));
        lineCount++;
    }
}
