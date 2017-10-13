package org.sagebionetworks.bridge.exporter.synapse;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test for transfer() in TransferMethod
 */
public class TransferMethodTest {

    @Test
    public void testTransfer() {
        final String testStringName = "test_string";
        final String testStringSetName = "test_string_set";
        final String testDateName = "test_date";

        // create mock record
        Item testRecord = new Item();
        testRecord.withString(testStringName, "test_string_value");
        testRecord.withStringSet(testStringSetName, "test_string_set_value_1", "test_string_set_value_2");
        testRecord.withLong(testDateName, 1484181511);
        testRecord.withString("largetext", "This is a small largetext");

        // verify
        assertEquals(TransferMethod.STRING.transfer(testStringName, testRecord), "test_string_value");
        assertEquals(TransferMethod.STRINGSET.transfer(testStringSetName, testRecord), "test_string_set_value_1,test_string_set_value_2");
        assertEquals(TransferMethod.DATE.transfer(testDateName, testRecord), "1484181511");
        assertEquals(TransferMethod.LARGETEXT.transfer("largetext", testRecord), "This is a small largetext");
    }

    // branch coverage
    @Test
    public void transferStringSetWithNullValue() {
        Item emptyRecord = new Item();
        assertEquals(TransferMethod.STRINGSET.transfer("no-value", emptyRecord), "");
    }

    @Test
    public void testGetColumnType() {
        assertEquals(TransferMethod.STRING.getColumnType(), ColumnType.STRING);
        assertEquals(TransferMethod.STRINGSET.getColumnType(), ColumnType.STRING);
        assertEquals(TransferMethod.DATE.getColumnType(), ColumnType.DATE);
        assertEquals(TransferMethod.LARGETEXT.getColumnType(), ColumnType.LARGETEXT);
    }
}
