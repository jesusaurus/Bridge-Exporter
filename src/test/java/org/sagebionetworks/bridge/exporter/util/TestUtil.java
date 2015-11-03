package org.sagebionetworks.bridge.exporter.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class TestUtil {
    public static List<String> bytesToLines(byte[] bytes) throws IOException {
        // Use Guava CharStreams to offload the complexities of parsing lines.
        try (StringReader stringReader = new StringReader(new String(bytes, Charsets.UTF_8))) {
            return CharStreams.readLines(stringReader);
        }
    }
}
