package org.sagebionetworks.bridge.exporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

public class UploadSchemaHelper {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Map<UploadSchemaKey, Item> schemaMap = new HashMap<>();
    private final Map<UploadSchemaKey, PrintWriter> exportWriterMap = new HashMap<>();

    private String dateString;
    private Table schemaTable;
    private File tmpDir;

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public void setSchemaTable(Table schemaTable) {
        this.schemaTable = schemaTable;
    }

    public void setTmpDir(File tmpDir) {
        this.tmpDir = tmpDir;
    }

    public void init() {
        Iterable<Item> schemaIter = schemaTable.scan();
        for (Item oneSchema : schemaIter) {
            String schemaTableKey = oneSchema.getString("key");
            String[] parts = schemaTableKey.split(":", 2);
            String studyId = parts[0];
            String schemaId = parts[1];

            int schemaRev = oneSchema.getInt("revision");
            UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

            schemaMap.put(schemaKey, oneSchema);
        }
    }

    public Item getSchema(UploadSchemaKey schemaKey) {
        return schemaMap.get(schemaKey);
    }

    public PrintWriter getExportWriter(UploadSchemaKey schemaKey) throws IOException {
        PrintWriter writer = exportWriterMap.get(schemaKey);
        if (writer != null) {
            return writer;
        }

        // init new writer
        String filename = schemaKey.getStudyId() + "-" + schemaKey.getSchemaId() + "-v" + schemaKey.getRev()
                + "-" + dateString + ".tsv";
        File file = new File(tmpDir, filename);
        OutputStream stream = new FileOutputStream(file);
        writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream, Charsets.UTF_8)));
        exportWriterMap.put(schemaKey, writer);

        // write header files
        Item schema = getSchema(schemaKey);
        writer.print("recordId\thealthCode\tuploadDate\tcreatedOn\tmetadata\tappName\tappVersion\tphoneInfo");

        JsonNode fieldDefList = JSON_MAPPER.readTree(schema.getString("fieldDefinitions"));
        for (JsonNode oneFieldDef : fieldDefList) {
            String name = oneFieldDef.get("name").textValue().replace('.', '_');
            writer.print("\t");
            writer.print(name);
        }
        writer.println();

        return writer;
    }

    public void closeAllFileHandlers() {
        for (PrintWriter oneWriter : exportWriterMap.values()) {
            try {
                Closeables.close(oneWriter, true);
            } catch (IOException ex) {
                // never happens
            }
        }

        exportWriterMap.clear();
    }
}
