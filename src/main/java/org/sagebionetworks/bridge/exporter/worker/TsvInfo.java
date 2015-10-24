package org.sagebionetworks.bridge.exporter.worker;

import java.io.File;
import java.io.PrintWriter;

// TODO doc
public class TsvInfo {
    private final File file;
    private final PrintWriter writer;

    private int lineCount = 0;

    public TsvInfo(File file, PrintWriter writer) {
        this.file = file;
        this.writer = writer;
    }

    public File getFile() {
        return file;
    }

    public PrintWriter getWriter() {
        return writer;
    }

    public int getLineCount() {
        return lineCount;
    }

    public int incrementLineCount() {
        return ++lineCount;
    }
}
