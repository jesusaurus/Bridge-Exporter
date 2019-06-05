package org.sagebionetworks.bridge.exporter.worker;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.SchemalessExportHandler;

public class MetaTableTypeTest {
    @Test
    public void createHandlerForType() {
        assertTrue(MetaTableType.APP_VERSION.createHandlerForType() instanceof AppVersionExportHandler);
        assertTrue(MetaTableType.DEFAULT.createHandlerForType() instanceof SchemalessExportHandler);
    }
}
