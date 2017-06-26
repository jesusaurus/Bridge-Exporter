package org.sagebionetworks.bridge.exporter.record;

import org.joda.time.DateTime;

public enum ExportType {
    DAILY {
        public DateTime getStartDateTime(DateTime endDateTime) {
            return endDateTime.minusDays(1).withTimeAtStartOfDay();
        }
    },
    HOURLY {
        public DateTime getStartDateTime(DateTime endDateTime) {
            return endDateTime.minusHours(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        }
    },
    INSTANT {
        public DateTime getStartDateTime(DateTime endDateTime) {
            return endDateTime.minusDays(1).withTimeAtStartOfDay();
        }
    };

    /**
     * Helper method to get the startDateTime given endDateTime determining by the type of the request
     * @param endDateTime
     * @return
     */
    public abstract DateTime getStartDateTime(DateTime endDateTime);
}
