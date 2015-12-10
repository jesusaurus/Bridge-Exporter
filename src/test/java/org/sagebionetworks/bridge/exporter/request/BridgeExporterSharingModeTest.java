package org.sagebionetworks.bridge.exporter.request;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.dynamo.SharingScope;

public class BridgeExporterSharingModeTest {
    @DataProvider(name = "sharingModeTestParams")
    public Object[][] sharingModeTestParams() {
        return new Object[][] {
                { BridgeExporterSharingMode.ALL, SharingScope.NO_SHARING, false },
                { BridgeExporterSharingMode.ALL, SharingScope.SPONSORS_AND_PARTNERS, false },
                { BridgeExporterSharingMode.ALL, SharingScope.ALL_QUALIFIED_RESEARCHERS, false },
                { BridgeExporterSharingMode.SHARED, SharingScope.NO_SHARING, true },
                { BridgeExporterSharingMode.SHARED, SharingScope.SPONSORS_AND_PARTNERS, false },
                { BridgeExporterSharingMode.SHARED, SharingScope.ALL_QUALIFIED_RESEARCHERS, false },
                { BridgeExporterSharingMode.PUBLIC_ONLY, SharingScope.NO_SHARING, true },
                { BridgeExporterSharingMode.PUBLIC_ONLY, SharingScope.SPONSORS_AND_PARTNERS, true },
                { BridgeExporterSharingMode.PUBLIC_ONLY, SharingScope.ALL_QUALIFIED_RESEARCHERS, false },
        };
    }

    @Test(dataProvider = "sharingModeTestParams")
    public void test(BridgeExporterSharingMode sharingMode, SharingScope sharingScope, boolean expected) {
        assertEquals(sharingMode.shouldExcludeScope(sharingScope), expected, "sharingMode=" + sharingMode +
                ", sharingScope=" + sharingScope);
    }

    // branch coverage test to satisfy jacoco
    @Test
    public void valueOf() {
        assertEquals(BridgeExporterSharingMode.valueOf("ALL"), BridgeExporterSharingMode.ALL);
        assertEquals(BridgeExporterSharingMode.valueOf("SHARED"), BridgeExporterSharingMode.SHARED);
        assertEquals(BridgeExporterSharingMode.valueOf("PUBLIC_ONLY"), BridgeExporterSharingMode.PUBLIC_ONLY);
    }
}
