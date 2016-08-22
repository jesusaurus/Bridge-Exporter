package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;

import java.util.concurrent.Future;

import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class ExportSubtaskFutureTest {
    // use mocks to avoid building complex objects just for a simple builder test
    private static final Future<Void> MOCK_FUTURE = mock(Future.class);
    private static final ExportSubtask MOCK_SUBTASK = mock(ExportSubtask.class);

    @Test
    public void normalCase() {
        ExportSubtaskFuture subtaskFuture = new ExportSubtaskFuture.Builder().withFuture(MOCK_FUTURE)
                .withSubtask(MOCK_SUBTASK).build();
        assertSame(subtaskFuture.getFuture(), MOCK_FUTURE);
        assertSame(subtaskFuture.getSubtask(), MOCK_SUBTASK);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "future must be specified")
    public void nullFuture() {
        new ExportSubtaskFuture.Builder().withSubtask(MOCK_SUBTASK).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "subtask must be specified")
    public void nullSubtask() {
        new ExportSubtaskFuture.Builder().withFuture(MOCK_FUTURE).build();
    }
}
