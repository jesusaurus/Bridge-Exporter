package org.sagebionetworks.bridge.exporter;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

public class DynamoDbHelper {
    private final DynamoDB ddbClient;

    public DynamoDbHelper(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    public ItemCollection<QueryOutcome> getUploadsForDate(LocalDate uploadDate) {
        String uploadDateString = uploadDate.toString(ISODateTimeFormat.date());
        Table uploadTable = ddbClient.getTable("local-DwayneJeng-Upload2");
        Index uploadDateIndex = uploadTable.getIndex("uploadDate-index");
        QuerySpec query = new QuerySpec().withHashKey("uploadDate", uploadDateString).withQueryFilters(
                new QueryFilter("complete").eq(1));
        ItemCollection<QueryOutcome> ddbResult = uploadDateIndex.query(query);
        return ddbResult;
    }

    public String getStudyForHealthcode(String healthCode) {
        Table healthCodeTable = ddbClient.getTable("local-DwayneJeng-HealthCode");
        ItemCollection<QueryOutcome> ddbResult = healthCodeTable.query("code", healthCode);
        Iterator<Item> itemIter = ddbResult.iterator();
        if (!itemIter.hasNext()) {
            throw new RuntimeException("no healthcode in DDB");
        }
        Item item = itemIter.next();
        String studyId = item.getString("studyIdentifier");
        if (Strings.isNullOrEmpty(studyId)) {
            throw new RuntimeException("no study ID");
        }
        return studyId;
    }
}
