package org.sagebionetworks.bridge.exporter.handler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.testng.annotations.Test;

public class PhoneAppVersionInfoTest {
    @Test
    public void noRecordMetadata() {
        Item record = new Item().withString("id", "test-record-id");
        PhoneAppVersionInfo info = PhoneAppVersionInfo.fromRecord(record);
        assertNull(info.getAppVersion());
        assertNull(info.getPhoneInfo());
    }

    @Test
    public void malformedJson() {
        Item record = new Item().withString("id", "test-record-id").withString("metadata", "this is bad json");
        PhoneAppVersionInfo info = PhoneAppVersionInfo.fromRecord(record);
        assertNull(info.getAppVersion());
        assertNull(info.getPhoneInfo());
    }

    @Test
    public void normalCase() {
        String metadataText = "{\n" +
                "   \"appVersion\":\"Bridge-EX 2.0\",\n" +
                "   \"phoneInfo\":\"My Debugger\"\n" +
                "}";
        Item record = new Item().withString("id", "test-record-id").withString("metadata", metadataText);
        PhoneAppVersionInfo info = PhoneAppVersionInfo.fromRecord(record);
        assertEquals(info.getAppVersion(), "Bridge-EX 2.0");
        assertEquals(info.getPhoneInfo(), "My Debugger");
    }

    @Test
    public void sanitizeFields() {
        String metadataText = "{\n" +
                "   \"appVersion\":\"<p>Bridge-EX 2.0</p>\",\n" +
                "   \"phoneInfo\":\"<b>formatting tag</b>\"\n" +
                "}";
        Item record = new Item().withString("id", "test-record-id").withString("metadata", metadataText);
        PhoneAppVersionInfo info = PhoneAppVersionInfo.fromRecord(record);
        assertEquals(info.getAppVersion(), "Bridge-EX 2.0");
        assertEquals(info.getPhoneInfo(), "formatting tag");
    }
}
