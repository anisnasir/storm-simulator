package streamitem;

import static org.junit.Assert.assertEquals;

import input.StreamItem;
import org.junit.Test;

public class StreamItemTest {
    private static final StreamItem item = new StreamItem(10, "Hello",1);

    @Test
    public void testToString() {
        assertEquals("Hello", item.getTaskID());
        
    }
    @Test
    public void testTimeStamp() {
        assertEquals(10, item.getTimestamp()); 
    }
    
    @Test
    public void testProcessingTime() {
        assertEquals(1, item.getProcessingTime()); 
    }
    @Test
    public void testFinishedTime() {
    	item.setFinishedTime(20);
        assertEquals(20, item.getFinishedTime()); 
    }
}
