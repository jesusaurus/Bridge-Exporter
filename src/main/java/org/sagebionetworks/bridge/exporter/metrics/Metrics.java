package org.sagebionetworks.bridge.exporter.metrics;

import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;

/** Helper object to collect metrics for a given Bridge-EX run. */
public class Metrics {
    private final SortedMultiset<String> counterMap = TreeMultiset.create();
    private final SortedSetMultimap<String, String> keyValuesMap = TreeMultimap.create();
    private final SortedSetMultimap<String, String> setCounterMap = TreeMultimap.create();

    /**
     * Returns an immutable copy of the counter map. Because the returned counter map and the backing original counter
     * map both use a SortedMultiset, this allows you to iterate the keys (and therefore counters) in sorted order, for
     * ease of display.
     */
    public synchronized SortedMultiset<String> getCounterMap() {
        return ImmutableSortedMultiset.copyOfSorted(counterMap);
    }

    /**
     * Increments the given counter by 1. If the counter doesn't exist, it initializes that counter with value 1.
     *
     * @param name
     *         name of the counter to increment
     * @return value of the counter, after increment
     */
    public synchronized int incrementCounter(String name) {
        counterMap.add(name);
        return counterMap.count(name);
    }

    /**
     * Returns a copy of the key value mapping. Note that this is backed by a TreeMultimap, so the keys and the values
     * will be in sorted order. However, there is no Guava equivalent for ImmutableTreeMultimap, so the returned copy
     * will be mutable. The returned copy will be a copy, and modifications to this copy will not affect the original.
     */
    public synchronized SortedSetMultimap<String, String> getKeyValuesMap() {
        return TreeMultimap.create(keyValuesMap);
    }

    /**
     * <p>
     * Adds the given name-value pair to the key-value mapping. A key can be associated with multiple values. This is
     * used for things like app versions per study or schemas not found.
     * </p>
     * <p>
     * Note that this is backed by a set, so assigning the same value to the same key multiple times will only put a
     * single key-value pair into the mapping.
     * </p>
     *
     * @param name
     *         key name to add to the key-value pair mapping
     * @param value
     *         value to be associated with the key
     * @return number of unique keys associated with the name, after adding the new value
     */
    public synchronized int addKeyValuePair(String name, String value) {
        keyValuesMap.put(name, value);
        return keyValuesMap.get(name).size();
    }

    // It's worth noting that key-value pairs and set-counter map have the same implementation. However, they have
    // different semantics and are used in different ways. It's cleaner to keep them separate, even though they're
    // identical, then try to merge them together and track which keys are just for counts and which keys are for
    // key-value pairs.

    /**
     * Returns a copy of the set-counter map. Similar to {@link #getKeyValuesMap}, this is backed by a TreeMultimap.
     */
    public synchronized SortedSetMultimap<String, String> getSetCounterMap() {
        return TreeMultimap.create(setCounterMap);
    }

    /**
     * Adds the value to the given counter in the set-counter map. This is used for when you want to count unique
     * instances of things, but don't care about the actual values, like counting unique health codes per study.
     *
     * @param name
     *         name of the set-counter to increment, if the new value isn't already in the set-counter
     * @param value
     *         value to add to the set-counter
     * @return number of unique values in the set-counter, after incrementing with the new value
     */
    public synchronized int incrementSetCounter(String name, String value) {
        setCounterMap.put(name, value);
        return setCounterMap.get(name).size();
    }
}
