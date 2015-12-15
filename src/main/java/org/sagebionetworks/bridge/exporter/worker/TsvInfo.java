package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.io.PrintWriter;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

/**
 * Helper class that keeps track of a TSV file, the writer that writes to the file, and a method for tracking and
 * incrementing TSV line counts.
 */
public class TsvInfo {
    private final File file;
    private final PrintWriter writer;

    private int lineCount = 0;

    /**
     * TSV info constructor.
     *
     * @param file
     *         TSV file
     * @param writer
     *         writer for the TSV file
     */
    public TsvInfo(File file, PrintWriter writer) {
        this.file = file;
        this.writer = writer;
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

    /** Writes the line to the TSV writer and increments the line count. Automatically appends a newline. */
    public synchronized void writeLine(String line) {
        writer.println(line);
        lineCount++;
    }
}
