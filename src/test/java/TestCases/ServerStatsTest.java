package TestCases;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import Input.StreamItem;
import Server.ServerStats;

public class ServerStatsTest {
    @Test
    public void testNumFinishedTasks() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20, new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals(1, stats.getNumFinishedTasks());
    }
    @Test
    public void testExecuteLatency() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20,new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals(10, (int)stats.getAverageExecuteLatency());
    }
    @Test
    public void testCapacity() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20,new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals(20, (int)stats.getCapacity());
    }
    @Test
    public void testProcessed() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20,new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals(10, (int)stats.getTotalProcessed());
    }
    
    @Test
    public void testResourceUtilization() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20,new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals(0.5, stats.getResourceUtilization(),0.02);
    }
    
    @Test
    public void testSerializable() {
        // double numWords = 1E6;
    	ServerStats stats = new ServerStats(20,new LinkedList<StreamItem<String>>());
        StreamItem<String> tempItem = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
        tempItem.setFinishedTime(System.currentTimeMillis()+10);
        stats.addTask(tempItem);
        assertEquals("1 10.0 10 0.5", stats.toString());
    }
}
