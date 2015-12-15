package org.sagebionetworks.bridge.exporter.record;

import static org.testng.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class RecordIdSourceTest {
    private static final RecordIdSource.Converter<String> TEST_CONVERTER = from -> from + "-converted";

    @Test
    public void test() {
        // make source
        List<String> testIterable = ImmutableList.of("foo", "bar", "baz");
        Iterable<String> recordIdIter = new RecordIdSource<>(testIterable, TEST_CONVERTER);

        // iterate and validate
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "foo-converted");
        assertEquals(recordIdList.get(1), "bar-converted");
        assertEquals(recordIdList.get(2), "baz-converted");
    }
}
