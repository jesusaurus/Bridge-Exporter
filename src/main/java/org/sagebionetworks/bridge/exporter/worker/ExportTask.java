package org.sagebionetworks.bridge.exporter.worker;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;

// TODO
public class ExportTask {
    private final Queue<Future<?>> subtaskFutureQueue = new LinkedList<>();
    private final Set<String> schemasNotFound = new HashSet<>();

    public void addSubtaskFuture(Future<?> subtaskFuture) {
        subtaskFutureQueue.add(subtaskFuture);
    }
}
