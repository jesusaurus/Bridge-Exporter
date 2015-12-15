package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.io.PrintWriter;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;

public class TsvInfoTest {
    private File mockFile;
    private PrintWriter mockWriter;
    private TsvInfo tsvInfo;

    @BeforeMethod
    public void before() {
        mockFile = mock(File.class);
        mockWriter = mock(PrintWriter.class);
        tsvInfo = new TsvInfo(mockFile, mockWriter);
    }

    @Test
    public void happyCase() throws Exception {
        // set up mocks
        when(mockWriter.checkError()).thenReturn(false);

        // write some lines
        tsvInfo.writeLine("foo");
        tsvInfo.writeLine("bar");
        tsvInfo.writeLine("baz");
        tsvInfo.flushAndCloseWriter();

        // validate TSV info fields
        assertSame(tsvInfo.getFile(), mockFile);
        assertEquals(tsvInfo.getLineCount(), 3);

        // validate writer
        verify(mockWriter).println("foo");
        verify(mockWriter).println("bar");
        verify(mockWriter).println("baz");
        verify(mockWriter).flush();
        verify(mockWriter).checkError();
        verify(mockWriter).close();
    }

    @Test(expectedExceptions = BridgeExporterException.class, expectedExceptionsMessageRegExp =
            "TSV writer has unknown error")
    public void writerError() throws Exception {
        when(mockWriter.checkError()).thenReturn(true);
        tsvInfo.writeLine("write a line for realism");
        tsvInfo.flushAndCloseWriter();
    }
}
