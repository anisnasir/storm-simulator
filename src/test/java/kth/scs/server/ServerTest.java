package kth.scs.server;

import static org.junit.Assert.*;

import org.junit.Test;

import kth.scs.input.StreamItem;
import kth.scs.server.Server;
import kth.scs.server.TimeGranularity;

public class ServerTest {
	@Test
	public void testBasic() {
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 20);
		assertEquals(0,server.getQueueSize());
		
	}
	@Test
	public void testProcessItem() {
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 20);
		StreamItem<String> item1 = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
		server.process(System.currentTimeMillis(), item1);
		
		assertEquals(1,server.getQueueSize());
		
	}
	@Test
	public void testProcessItems1() {
		//testing the input when the timestamp does not allow processing the first task
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 20);
		StreamItem<String> item1 = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
		server.process(System.currentTimeMillis(), item1);
		
		StreamItem<String> item2 = new StreamItem<String>(System.currentTimeMillis(), "anis2", 10);
		server.process(System.currentTimeMillis(), item2);
		assertEquals(2,server.getQueueSize());
	}
	@Test
	public void testProcessItems2() {
		//testing the input when the timestamp does allow processing the first task
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 15);
		StreamItem<String> item1 = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
		server.process(System.currentTimeMillis(), item1);
		
		StreamItem<String> item2 = new StreamItem<String>(System.currentTimeMillis(), "anis2", 10);
		server.process(System.currentTimeMillis()+60, item2);
		assertEquals(1,server.getQueueSize());
		assertEquals(2,server.getStatsSize());
	}
	@Test
	public void testProcessItems3() {
		//testing the input when the timestamp allows processing the two task
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 20);
		StreamItem<String> item1 = new StreamItem<String>(System.currentTimeMillis(), "anis", 10);
		server.process(System.currentTimeMillis(), item1);
		
		StreamItem<String> item2 = new StreamItem<String>(System.currentTimeMillis(), "anis2", 10);
		server.process(System.currentTimeMillis()+25, item2);
		assertEquals(1,server.getQueueSize());
	}
	@Test
	public void testServerStats() {
		//testing the input when the timestamp allows processing the two task
		Server server = new Server(0, System.currentTimeMillis(), TimeGranularity.MINUTE,1, 100);
		StreamItem<String> item1 = new StreamItem<String>(System.currentTimeMillis(), "anis", 60);
		server.process(System.currentTimeMillis(), item1);
		
		StreamItem<String> item2 = new StreamItem<String>(System.currentTimeMillis(), "anis2", 40);
		server.process(System.currentTimeMillis()+60, item2);
		assertEquals(2,server.getStatsSize());
	}

}
