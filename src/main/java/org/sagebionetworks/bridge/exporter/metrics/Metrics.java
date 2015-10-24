package org.sagebionetworks.bridge.exporter.metrics;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;

// TODO doc
public class Metrics {
    private final Multiset<String> counterMap = TreeMultiset.create();
    private final Multimap<String, String> keyValuesMap = TreeMultimap.create();
    private final Multimap<String, String> setCounterMap = TreeMultimap.create();

    public Multiset<String> getCounterMap() {
        return counterMap;
    }

    public int incrementCounter(String name) {
        counterMap.add(name);
        return counterMap.count(name);
    }

    // Metrics value sets under a metrics name. Used for things like app versions or schemas not found.
    public Multimap<String, String> getKeyValuesMap() {
        return keyValuesMap;
    }

    public int addKeyValuePair(String name, String value) {
        keyValuesMap.put(name, value);
        return keyValuesMap.get(name).size();
    }

    // Only increments the counter if the value hasn't already been used. Used for things like counting unique health
    // codes.
    public Multimap<String, String> getSetCounterMap() {
        return setCounterMap;
    }

    public int incrementSetCounter(String name, String value) {
        setCounterMap.put(name, value);
        return setCounterMap.get(name).size();
    }
}
