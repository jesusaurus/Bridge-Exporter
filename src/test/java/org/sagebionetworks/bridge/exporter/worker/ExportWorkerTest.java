package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.handler.ExportHandler;

// ExportWorker is a fairly trivial class. This class exists mainly for unit test coverage.
public class ExportWorkerTest {
    @Test
    public void test() {
        // mock handler and subtask to simplify testing
        ExportHandler mockHandler = mock(ExportHandler.class);
        ExportSubtask mockSubtask = mock(ExportSubtask.class);

        // execute and verify call-through
        new ExportWorker(mockHandler, mockSubtask).run();
        verify(mockHandler).handle(mockSubtask);
    }
}
