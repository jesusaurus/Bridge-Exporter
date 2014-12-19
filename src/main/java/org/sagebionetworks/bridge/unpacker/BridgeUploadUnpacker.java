package org.sagebionetworks.bridge.unpacker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

public class BridgeUploadUnpacker {
    private static final BridgeUploadUnpacker INSTANCE = new BridgeUploadUnpacker();
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private static final String KEY_INFO_JSON = "info.json";
    private static final Set<String> KEYSET_SURVEY_RESPONSE = ImmutableSet.of("endDate", "identifier", "questionType",
            "startDate");

    @SuppressWarnings("unchecked")
    public void unpack(InputStream stream) throws ClassCastException, IOException {
        // parse input stream as JSON
        Map<String, Object> rootMap = JSON_OBJECT_MAPPER.readValue(stream, Map.class);

        // parse info.json
        Map<String, Object> infoMap = (Map<String, Object>) rootMap.get(KEY_INFO_JSON);
        String itemName = (String) infoMap.get("item");
        List<Object> fileList = (List<Object>) infoMap.get("files");
        String taskRunId = (String) infoMap.get("taskRun");

        // filter out the "no data" entries
        UploadSubformat subformat = null;
        if (fileList.isEmpty()) {
            subformat = UploadSubformat.NO_DATA;
        }

        if (subformat == null) {
            // try to identify type based on "info.json" -> "item"
            subformat = UploadSubformat.fromItemName(itemName);
        }

        if (subformat == null) {
            // Unable to parse by itemName. Try inspecting some of the items.
            for (Map.Entry<String, Object> oneRootEntry : rootMap.entrySet()) {
                if (KEY_INFO_JSON.equals(oneRootEntry.getKey())) {
                    // This is a special metadata entry. Skip it.
                    continue;
                }

                Map<String, Object> fileMap = (Map<String, Object>) oneRootEntry.getValue();
                if (fileMap.keySet().containsAll(KEYSET_SURVEY_RESPONSE)) {
                    // Matches survey response schema.
                    subformat = UploadSubformat.SURVEY_RESPONSE;
                }

                // We only need to inspect one (non-info.json) element to determine if it's a survey or not. The loop
                // is really just to bypass info.json.
                break;
            }
        }

        if (subformat != null) {
            System.out.println(subformat);
        } else {
            System.out.println(String.format("Unknown subformat for item='%s', taskRun='%s'", itemName, taskRunId));
        }
    }

    public static void main(String[] args) throws IOException {
        // each arg is a filename
        for (String oneArg : args) {
            try (FileInputStream fileInStream = new FileInputStream(oneArg)) {
                INSTANCE.unpack(fileInStream);
            } catch (ClassCastException | IOException ex) {
                System.out.println(String.format("Exception thrown for file '%s':", oneArg));
                ex.printStackTrace();
            }
        }
    }
}
