package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

public class PhoneAppVersionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneAppVersionInfo.class);

    private final String appVersion;
    private final String phoneInfo;

    public static PhoneAppVersionInfo fromRecord(Item record) {
        String recordId = record.getString("id");

        String appVersion = null;
        String phoneInfo = null;
        String metadataString = record.getString("metadata");
        if (StringUtils.isNotBlank(metadataString)) {
            try {
                JsonNode metadataJson = DefaultObjectMapper.INSTANCE.readTree(metadataString);
                appVersion = BridgeExporterUtil.sanitizeJsonValue(metadataJson, "appVersion", 48,
                        recordId);
                phoneInfo = BridgeExporterUtil.sanitizeJsonValue(metadataJson, "phoneInfo", 48, recordId);
            } catch (IOException ex) {
                // we can recover from this
                LOG.error("Error parsing metadata for record ID " + recordId + ": " + ex.getMessage(), ex);
            }
        }

        return new PhoneAppVersionInfo(appVersion, phoneInfo);
    }

    public PhoneAppVersionInfo(String appVersion, String phoneInfo) {
        this.appVersion = appVersion;
        this.phoneInfo = phoneInfo;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getPhoneInfo() {
        return phoneInfo;
    }
}
