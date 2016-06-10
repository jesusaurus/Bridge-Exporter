package org.sagebionetworks.bridge.exporter.handler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HealthDataExportHandlerTest {
    private static final String FIELD_NAME = "foo-field";
    private static final String FIELD_NAME_TIMEZONE = FIELD_NAME + ".timezone";

    // branch coverage
    @Test
    public void nullNode() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME, null);
        assertTrue(rowValueMap.isEmpty());
    }

    // branch coverage
    @Test
    public void javaNull() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                NullNode.instance);
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void invalidType() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                BooleanNode.TRUE);
        assertTrue(rowValueMap.isEmpty());
    }

    @Test
    public void malformedTimestampString() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new TextNode("Thursday June 9th 2016 @ 4:10pm"));
        assertTrue(rowValueMap.isEmpty());
    }

    @DataProvider(name = "timestampStringDataProvider")
    public Object[][] timestampStringDataProvider() {
        // { timestampString, expectedTimezoneString }
        return new Object[][] {
                { "2016-06-09T12:34:56.789Z", "+0000" },
                { "2016-06-09T01:02:03.004+0900", "+0900" },
                { "2016-06-09T02:03:05.007-0700", "-0700" },
                { "2016-06-09T10:09:08.765+0530", "+0530" },
        };
    }

    @Test(dataProvider = "timestampStringDataProvider")
    public void timestampString(String timestampString, String expectedTimezoneString) {
        long expectedMillis = DateTime.parse(timestampString).getMillis();

        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new TextNode(timestampString));

        assertEquals(rowValueMap.size(), 2);
        assertEquals(rowValueMap.get(FIELD_NAME), String.valueOf(expectedMillis));
        assertEquals(rowValueMap.get(FIELD_NAME_TIMEZONE), expectedTimezoneString);
    }

    @Test
    public void epochMillis() {
        Map<String, String> rowValueMap = HealthDataExportHandler.serializeTimestamp("dummy", FIELD_NAME,
                new IntNode(12345));

        assertEquals(rowValueMap.size(), 2);
        assertEquals(rowValueMap.get(FIELD_NAME), "12345");
        assertEquals(rowValueMap.get(FIELD_NAME_TIMEZONE), "+0000");
    }
}
