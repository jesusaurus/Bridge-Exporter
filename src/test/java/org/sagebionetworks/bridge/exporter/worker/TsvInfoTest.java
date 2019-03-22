package org.sagebionetworks.bridge.exporter.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Writer;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterTsvException;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;

public class TsvInfoTest {
    private static final List<String> COLUMN_NAME_LIST = ImmutableList.of("foo", "bar");
    private static final String TEST_RECORD_ID = "test record id";

    private InMemoryFileHelper inMemoryFileHelper;
    private File tsvFile;
    private TsvInfo tsvInfo;

    @BeforeMethod
    public void before() throws Exception {
        inMemoryFileHelper = new InMemoryFileHelper();
        File tmpDir = inMemoryFileHelper.createTempDir();
        tsvFile = inMemoryFileHelper.newFile(tmpDir, "test.tsv");
        Writer tsvWriter = inMemoryFileHelper.getWriter(tsvFile);
        tsvInfo = new TsvInfo(COLUMN_NAME_LIST, tsvFile, tsvWriter);
    }

    @Test
    public void happyCase() throws Exception {
        // write some lines
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("foo", "foo value").put("bar", "bar value")
                .build());
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("foo", "second foo value")
                .put("extraneous", "extraneous value").put("bar", "second bar value").build());
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("bar", "has bar but not foo").build());
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>()
                .put("foo", "newlines\n\ncrlf\r\ntabs\t\tend of line")
                .put("bar", "quotes\"escaped quotes\\\"end of line").build());
        tsvInfo.addRecordId(TEST_RECORD_ID);
        tsvInfo.flushAndCloseWriter();

        // validate TSV info fields
        assertSame(tsvInfo.getFile(), tsvFile);
        assertEquals(tsvInfo.getLineCount(), 4);
        assertEquals(tsvInfo.getRecordIds().get(0), TEST_RECORD_ID);

        // Validate TSV File.
        // A few quirks about CSVWriter (all of which are in our test space):
        // * If the value is empty, it doesn't quote it.
        // * It escapes quotes by double-quoting (" -> "").
        // * It escapes slashes by double-slashing (\ -> \\).
        // * It doesn't escape anything else (newlines, carriage returns, tabs, etc).
        String expectedFileContents = "\"foo\"\t\"bar\"\n" +
                "\"foo value\"\t\"bar value\"\n" +
                "\"second foo value\"\t\"second bar value\"\n" +
                "\t\"has bar but not foo\"\n" +
                "\"newlines\n\ncrlf\r\ntabs\t\tend of line\"\t\"quotes\"\"escaped quotes\\\\\"\"end of line\"\n";
        String actualFileContents = new String(inMemoryFileHelper.getBytes(tsvFile));
        assertEquals(actualFileContents, expectedFileContents);
    }

    @Test
    public void initError() {
        Exception testEx = new Exception();
        TsvInfo errorTsvInfo = new TsvInfo(testEx);

        try {
            errorTsvInfo.checkInitAndThrow();
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertTrue(ex instanceof BridgeExporterTsvException);
            assertSame(ex.getCause(), testEx);
        }

        try {
            errorTsvInfo.writeRow(ImmutableMap.of());
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertTrue(ex instanceof BridgeExporterTsvException);
            assertSame(ex.getCause(), testEx);
        }

        try {
            errorTsvInfo.flushAndCloseWriter();
            fail("expected exception");
        } catch (BridgeExporterException ex) {
            assertTrue(ex instanceof BridgeExporterTsvException);
            assertSame(ex.getCause(), testEx);
        }

        assertNull(errorTsvInfo.getFile());
        assertEquals(errorTsvInfo.getLineCount(), 0);
        assertEquals(errorTsvInfo.getRecordIds().size(), 0);
    }
}
