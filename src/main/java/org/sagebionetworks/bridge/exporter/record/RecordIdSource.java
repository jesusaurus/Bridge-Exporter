package org.sagebionetworks.bridge.exporter.record;

import java.util.Iterator;

// TODO doc
public class RecordIdSource<T> implements Iterable<String>, Iterator<String> {
    private final Iterator<T> sourceIterator;
    private final Converter<T> converter;

    public RecordIdSource(Iterable<T> sourceIterable, Converter<T> converter) {
        this(sourceIterable.iterator(), converter);
    }

    public RecordIdSource(Iterator<T> sourceIterator, Converter<T> converter) {
        this.sourceIterator = sourceIterator;
        this.converter = converter;
    }

    // This class implements iterable out of convenience. It itself is the iterator, so iterator() returns this.
    @Override
    public Iterator<String> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return sourceIterator.hasNext();
    }

    @Override
    public String next() {
        return converter.convert(sourceIterator.next());
    }

    public interface Converter<T> {
        String convert(T from);
    }
}
