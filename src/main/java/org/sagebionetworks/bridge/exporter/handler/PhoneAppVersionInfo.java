package org.sagebionetworks.bridge.exporter.handler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;

/** Encapsulates getting appVersion and phoneInfo from record metadata. */
public class PhoneAppVersionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneAppVersionInfo.class);

    private final String appVersion;
    private final String phoneInfo;

    /**
     * Creates a PhoneAppVersionInfo from the metadata of a health data record. Returns a Info with null fields if the
     * metadata can't be parsed.
     */
    public static PhoneAppVersionInfo fromRecord(Item record) {
        String recordId = record.getString("id");

        String appVersion = null;
        String phoneInfo = null;
        String metadataString = record.getString("metadata");
        if (StringUtils.isNotBlank(metadataString)) {
            try {
                JsonNode metadataJson = DefaultObjectMapper.INSTANCE.readTree(metadataString);
                appVersion = BridgeExporterUtil.sanitizeJsonValue(metadataJson, "appVersion", 48, recordId);
                phoneInfo = BridgeExporterUtil.sanitizeJsonValue(metadataJson, "phoneInfo", 48, recordId);
            } catch (IOException ex) {
                // We don't want callers to have to deal with boilerplate error handling code, so we log the error and
                // return null fields.
                LOG.error("Error parsing metadata for record ID " + recordId + ": " + ex.getMessage(), ex);
            }
        }

        return new PhoneAppVersionInfo(appVersion, phoneInfo);
    }

    /** Private constructor. All building goes through the fromRecord static factory method. */
    private PhoneAppVersionInfo(String appVersion, String phoneInfo) {
        this.appVersion = appVersion;
        this.phoneInfo = phoneInfo;
    }

    /** App version, as extracted from the record metadata. */
    public String getAppVersion() {
        return appVersion;
    }

    /** Phone Info (such as OS and hardware), as extracted from the record metadata. */
    public String getPhoneInfo() {
        return phoneInfo;
    }
}
