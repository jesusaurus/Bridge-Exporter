package org.sagebionetworks.bridge.exporter.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import com.google.common.collect.SortedMultiset;
import com.google.common.collect.SortedSetMultimap;
import org.testng.annotations.Test;

public class MetricsTest {
    @Test
    public void counters() {
        // init with some data
        Metrics metrics = new Metrics();
        assertEquals(metrics.incrementCounter("foo"), 1);
        assertEquals(metrics.incrementCounter("bar"), 1);
        assertEquals(metrics.incrementCounter("bar"), 2);
        assertEquals(metrics.incrementCounter("baz"), 1);
        assertEquals(metrics.incrementCounter("baz"), 2);
        assertEquals(metrics.incrementCounter("baz"), 3);

        // validate the counter map
        SortedMultiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.elementSet().size(), 3);
        assertEquals(counterMap.count("foo"), 1);
        assertEquals(counterMap.count("bar"), 2);
        assertEquals(counterMap.count("baz"), 3);
    }

    @Test
    public void keyValuePairs() {
        // init with some data
        Metrics metrics = new Metrics();
        assertEquals(metrics.addKeyValuePair("1value", "the only value"), 1);

        assertEquals(metrics.addKeyValuePair("uniqueValues", "value A"), 1);
        assertEquals(metrics.addKeyValuePair("uniqueValues", "value B"), 2);

        assertEquals(metrics.addKeyValuePair("duplicateValues", "value C"), 1);
        assertEquals(metrics.addKeyValuePair("duplicateValues", "value C"), 1);
        assertEquals(metrics.addKeyValuePair("duplicateValues", "value D"), 2);
        assertEquals(metrics.addKeyValuePair("duplicateValues", "value D"), 2);

        // validate the key-value pair map
        SortedSetMultimap<String, String> keyValuesMap = metrics.getKeyValuesMap();
        assertEquals(keyValuesMap.keySet().size(), 3);

        Set<String> oneValueSet = keyValuesMap.get("1value");
        assertEquals(oneValueSet.size(), 1);
        assertTrue(oneValueSet.contains("the only value"));

        Set<String> uniqueValuesSet = keyValuesMap.get("uniqueValues");
        assertEquals(uniqueValuesSet.size(), 2);
        assertTrue(uniqueValuesSet.contains("value A"));
        assertTrue(uniqueValuesSet.contains("value B"));

        Set<String> duplicateValuesSet = keyValuesMap.get("duplicateValues");
        assertEquals(duplicateValuesSet.size(), 2);
        assertTrue(duplicateValuesSet.contains("value C"));
        assertTrue(duplicateValuesSet.contains("value D"));

        // Because Guava has no ImmutableTreeMultimap, we manually verify that changes to the map don't affect the
        // original.
        keyValuesMap.put("foo", "bar");
        SortedSetMultimap<String, String> originalKeyValuesMap = metrics.getKeyValuesMap();
        assertFalse(originalKeyValuesMap.containsKey("foo"));
    }

    @Test
    public void setCounters() {
        // Note that this test is identical to keyValuePairs(). See comments on Metrics.java for details.

        // init with some data
        Metrics metrics = new Metrics();
        assertEquals(metrics.incrementSetCounter("1value", "the only value"), 1);

        assertEquals(metrics.incrementSetCounter("uniqueValues", "value A"), 1);
        assertEquals(metrics.incrementSetCounter("uniqueValues", "value B"), 2);

        assertEquals(metrics.incrementSetCounter("duplicateValues", "value C"), 1);
        assertEquals(metrics.incrementSetCounter("duplicateValues", "value C"), 1);
        assertEquals(metrics.incrementSetCounter("duplicateValues", "value D"), 2);
        assertEquals(metrics.incrementSetCounter("duplicateValues", "value D"), 2);

        // validate the key-value pair map
        SortedSetMultimap<String, String> setCounterMap = metrics.getSetCounterMap();
        assertEquals(setCounterMap.keySet().size(), 3);

        Set<String> oneValueSet = setCounterMap.get("1value");
        assertEquals(oneValueSet.size(), 1);
        assertTrue(oneValueSet.contains("the only value"));

        Set<String> uniqueValuesSet = setCounterMap.get("uniqueValues");
        assertEquals(uniqueValuesSet.size(), 2);
        assertTrue(uniqueValuesSet.contains("value A"));
        assertTrue(uniqueValuesSet.contains("value B"));

        Set<String> duplicateValuesSet = setCounterMap.get("duplicateValues");
        assertEquals(duplicateValuesSet.size(), 2);
        assertTrue(duplicateValuesSet.contains("value C"));
        assertTrue(duplicateValuesSet.contains("value D"));

        // Because Guava has no ImmutableTreeMultimap, we manually verify that changes to the map don't affect the
        // original.
        setCounterMap.put("foo", "bar");
        SortedSetMultimap<String, String> originalSetCounterMap = metrics.getSetCounterMap();
        assertFalse(originalSetCounterMap.containsKey("foo"));
    }
}
