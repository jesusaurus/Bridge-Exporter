package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.sagebionetworks.bridge.exceptions.ExportWorkerException;

/** Gets record IDs from a file. This is useful for redrives. */
public class FileRecordIdSource extends RecordIdSource {
    // Configured externally
    private String filename;

    // Internal state
    private Iterator<String> recordIdIter;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    @Override
    public void init() throws ExportWorkerException {
        File file = new File(filename);
        List<String> recordIdList;
        try {
            recordIdList = Files.readLines(file, Charsets.UTF_8);
        } catch (IOException ex) {
            throw new ExportWorkerException(ex);
        }
        recordIdIter = recordIdList.iterator();
    }

    @Override
    public boolean hasNext() {
        return recordIdIter.hasNext();
    }

    @Override
    public String next() {
        return recordIdIter.next();
    }
}
