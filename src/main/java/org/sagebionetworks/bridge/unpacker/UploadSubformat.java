package org.sagebionetworks.bridge.unpacker;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum UploadSubformat {
    CARDIO_WALK_TEST("6-Minute Walk Test"),
    DIABETES_GLUCOSE_LEVELS("Glucose Levels"),
    NO_DATA(),
    PARKINSONS_TAP_TEST("Tapping Activity", "Tapping Task"),
    PARKINSONS_WALK_TEST("Timed Walking Task"),
    SURVEY_RESPONSE();

    private final Set<String> itemNameSet;

    private UploadSubformat(String... itemNames) {
        itemNameSet = ImmutableSet.copyOf(itemNames);
    }

    public static UploadSubformat fromItemName(String itemName) {
        for (UploadSubformat oneSubformat : values()) {
            if (oneSubformat.itemNameSet.contains(itemName)) {
                return oneSubformat;
            }
        }

        // We return null, because there are lots of item names that we can't parse to an UploadSubformat (for example,
        // most of the SURVEY_RESPONSE types).
        return null;
    }
}
