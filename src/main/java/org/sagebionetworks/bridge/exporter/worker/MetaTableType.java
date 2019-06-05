package org.sagebionetworks.bridge.exporter.worker;

import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.SchemalessExportHandler;
import org.sagebionetworks.bridge.exporter.handler.SynapseExportHandler;

/** Enumerates the various type of meta-tables (ie, tables not associated with a schema). */
public enum MetaTableType {
    /** App-Version table, aka Health Data Summary Table. */
    APP_VERSION {
        @Override
        SynapseExportHandler createHandlerForType() {
            return new AppVersionExportHandler();
        }
    },

    /** Default Health Data Record Table, aka schemaless records table. */
    DEFAULT {
        @Override
        SynapseExportHandler createHandlerForType() {
            return new SchemalessExportHandler();
        }
    };

    /** Creates and returns an export handler corresponding to the table type. */
    abstract SynapseExportHandler createHandlerForType();
}
