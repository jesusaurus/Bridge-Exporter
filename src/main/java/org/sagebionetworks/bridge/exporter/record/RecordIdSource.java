package org.sagebionetworks.bridge.exporter.record;

import java.util.Iterator;

/**
 * Record ID source. This wraps around either a file or a DynamoDB query. This is used to abstract away implementation
 * details for how we get a list of record IDs.
 *
 * @param <T>
 *         Source type that we need to convert from. For example, a DynamoDB record source would be a
 *         RecordIdSource<Item>
 */
public class RecordIdSource<T> implements Iterable<String>, Iterator<String> {
    private final Iterator<T> sourceIterator;
    private final Converter<T> converter;

    /**
     * Constructor on an iterable and a converter.
     *
     * @param sourceIterable
     *         iterable to parse record IDs from
     * @param converter
     *         converter used to parse record IDs
     */
    public RecordIdSource(Iterable<T> sourceIterable, Converter<T> converter) {
        this(sourceIterable.iterator(), converter);
    }

    /**
     * Constructor on an iterable and a converter.
     *
     * @param sourceIterator
     *         iterator to parse record IDs from
     * @param converter
     *         converter used to parse record IDs
     */
    public RecordIdSource(Iterator<T> sourceIterator, Converter<T> converter) {
        this.sourceIterator = sourceIterator;
        this.converter = converter;
    }

    /** This class implements iterable out of convenience. It itself is the iterator, so iterator() returns this. */
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

    /**
     * Interface (function reference) for the converter, which converts the given object into a record ID.
     *
     * @param <T>
     *         param type to convert from
     */
    public interface Converter<T> {
        String convert(T from);
    }
}
