package org.sagebionetworks.bridge.exporter.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.SortedSetMultimap;
import org.testng.annotations.Test;

public class MetricsHelperTest {
    @Test
    public void captureMetricsForRecord() {
        // setup
        Item dummyRecord = new Item().withString("healthCode", "dummy-health-code")
                .withString("studyId", "test-study");
        Metrics metrics = new Metrics();

        // execute
        new MetricsHelper().captureMetricsForRecord(metrics, dummyRecord);

        // validate
        SortedSetMultimap<String, String> setCounterMap = metrics.getSetCounterMap();
        assertEquals(setCounterMap.keySet().size(), 1);

        Set<String> healthCodeSet = setCounterMap.get("uniqueHealthCodes[test-study]");
        assertEquals(healthCodeSet.size(), 1);
        assertTrue(healthCodeSet.contains("dummy-health-code"));
    }

    @Test
    public void publishMetrics() {
        // Note that since this writes to the logs, we can't actually verify anything. This test is mainly to
        // (a) we exercise the code and (b) line/branch coverage.

        // init some data
        Metrics metrics = new Metrics();

        metrics.incrementCounter("foo-counter");
        metrics.incrementCounter("bar-counter");

        metrics.incrementSetCounter("qwerty-set-counter", "qwerty value");
        metrics.incrementSetCounter("asdf-set-counter", "asdf value");

        metrics.addKeyValuePair("aaa-key", "aaa value");
        metrics.addKeyValuePair("bbb-key", "bbb value");

        // execute
        new MetricsHelper().publishMetrics(metrics);
    }
}
