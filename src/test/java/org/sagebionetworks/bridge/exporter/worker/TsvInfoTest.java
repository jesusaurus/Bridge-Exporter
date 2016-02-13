package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

public class TsvInfoTest {
    private static final List<String> COLUMN_NAME_LIST = ImmutableList.of("foo", "bar");

    private File mockFile;
    private PrintWriter mockWriter;
    private TsvInfo tsvInfo;

    @BeforeMethod
    public void before() {
        mockFile = mock(File.class);
        mockWriter = mock(PrintWriter.class);
        tsvInfo = new TsvInfo(COLUMN_NAME_LIST, mockFile, mockWriter);
    }

    @Test
    public void happyCase() throws Exception {
        // set up mocks
        when(mockWriter.checkError()).thenReturn(false);

        // write some lines
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("foo", "foo value").put("bar", "bar value")
                .build());
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("foo", "second foo value")
                .put("extraneous", "extraneous value").put("bar", "second bar value").build());
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("bar", "has bar but not foo").build());
        tsvInfo.flushAndCloseWriter();

        // validate TSV info fields
        assertSame(tsvInfo.getFile(), mockFile);
        assertEquals(tsvInfo.getLineCount(), 3);

        // validate writer
        verify(mockWriter).println("foo\tbar");
        verify(mockWriter).println("foo value\tbar value");
        verify(mockWriter).println("second foo value\tsecond bar value");
        verify(mockWriter).println("\thas bar but not foo");
        verify(mockWriter).flush();
        verify(mockWriter).checkError();
        verify(mockWriter).close();
    }

    @Test(expectedExceptions = BridgeExporterException.class, expectedExceptionsMessageRegExp =
            "TSV writer has unknown error")
    public void writerError() throws Exception {
        when(mockWriter.checkError()).thenReturn(true);
        tsvInfo.writeRow(new ImmutableMap.Builder<String, String>().put("foo", "realistic").put("bar", "lines")
                .build());
        tsvInfo.flushAndCloseWriter();
    }
}
