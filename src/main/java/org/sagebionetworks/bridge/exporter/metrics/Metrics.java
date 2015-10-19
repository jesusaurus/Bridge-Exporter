package org.sagebionetworks.bridge.exporter.metrics;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// TODO doc
public class Metrics {
    private final Map<String, Integer> counterMap = new TreeMap<>();
    private final Map<String, Set<String>> setCounterMap = new TreeMap<>();

    public int incrementCounter(String name) {
        Integer oldValue = counterMap.get(name);
        int newValue;
        if (oldValue == null) {
            newValue = 1;
        } else {
            newValue = oldValue + 1;
        }

        counterMap.put(name, newValue);
        return newValue;
    }

    // Only increments the counter if the value hasn't already been used. Used for things like counting unique health
    // codes.
    public int incrementSetCounter(String name, String value) {
        Set<String> set = setCounterMap.get(name);
        if (set == null) {
            set = new HashSet<>();
            setCounterMap.put(name, set);
        }
        set.add(value);
        return set.size();
    }
}
