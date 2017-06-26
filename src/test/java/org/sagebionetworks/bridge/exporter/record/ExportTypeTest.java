package org.sagebionetworks.bridge.exporter.record;

import static org.testng.Assert.assertEquals;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

public class ExportTypeTest {
    private static final String END_DATE_TIME_STR = "2016-05-09T23:59:59.999-0700";
    private static final String END_DATE_TIME_STR_DAILY = "2016-05-08T00:00:00.000-0700";
    private static final String END_DATE_TIME_STR_HOUR = "2016-05-09T22:00:00.000-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STR);
    private static final DateTime END_DATE_TIME_DAILY = DateTime.parse(END_DATE_TIME_STR_DAILY);
    private static final DateTime END_DATE_TIME_HOUR = DateTime.parse(END_DATE_TIME_STR_HOUR);

    @Test
    public void testGetStartDateTime() {
        ExportType testExportType = ExportType.DAILY;
        DateTime startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME_DAILY);

        testExportType = ExportType.HOURLY;
        startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME_HOUR);

        testExportType = ExportType.INSTANT;
        startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME_DAILY);
    }
}
