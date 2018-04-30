package kth.scs.partitioner;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;

import kth.scs.input.StreamItem;
import kth.scs.partitioner.ConsistentGroupingKG;
import kth.scs.partitioner.ConsistentGroupingPKG;
import kth.scs.server.Server;
import kth.scs.server.TimeGranularity;

public class ConsistentGroupingSGTest {

	@Test
	public void testIncreaseLoad() {
		Collection<Server> workers = new ArrayList<Server>();
		
		int numServer = 10;
		int numVirtualServers = 4;
		
		for(int i = 0 ; i< 10;i++) {
			workers.add(new Server(i, 0, TimeGranularity.MINUTE , 1, 10));
		}
		
		ConsistentGroupingPKG hash = new ConsistentGroupingPKG(workers,1, numVirtualServers);
		StreamItem<String> item = new StreamItem<String>(0, "anis", 1);
		Server server1 = hash.getServer(0, item);
		Server server2 = hash.getServer(0, item);
		//System.out.println(server1.getServerID() + " " + server2.getServerID());
		boolean flag = (server1.getServerID() == server2.getServerID());
		assertEquals(flag, false);
	}
/*	
	@Test
	public void testDecreaseLoad() {
		Collection<String> workers = new ArrayList<String>();
		for(int i = 0 ; i< 50;i++) {
			workers.add("worker"+i);
		}
		int numVirtualServers = 4;
		ConsistentHash<String> hash = new ConsistentHash<String>(workers, numVirtualServers);
		assertEquals(hash.getSize(), 200);
		
		hash.increaseLoadQueue("worker"+1);
		hash.decreaseLoadQueue("worker"+3);
		assertEquals(hash.getSize(), 200);
	}
	@Test
	public void testInsertKey() {
		Collection<String> workers = new ArrayList<String>();
		for(int i = 0 ; i< 50;i++) {
			workers.add("worker"+i);
		}
		int numVirtualServers = 4;
		ConsistentHash<String> hash = new ConsistentHash<String>(workers, numVirtualServers);
		String firstWorker = hash.getWorker(1);
		String secondWorker = hash.getWorker(1);
		assertEquals(firstWorker.equals(secondWorker), false);
	}
	@Test
	public void testInsertBulkZipf() {
		Collection<String> workers = new ArrayList<String>();
		int numWorkers = 50;
		for(int i = 0 ; i< numWorkers;i++) {
			workers.add("worker"+i);
		}
		int numVirtualServers = 4;
		ConsistentHash<String> hash = new ConsistentHash<String>(workers, numVirtualServers);
		int numUniqueItems = 100;
		double skew = 1.0;
		ZipfDistribution zipf = new ZipfDistribution(numUniqueItems, skew);
		int numElements = 1000;
		for(int i = 0;i< numElements; i++) {
			hash.getWorker(zipf.sample());
		}
		
	}
	
	@Test
	public void testHeterogenousCluster() {
		int[] capacity = {10, 50, 5, 55};
		int index =0 ;
		HashMap<String, Integer> capacities = new HashMap<String, Integer>();
		Collection<String> workers = new ArrayList<String>();
		int numWorkers = 50;
		for(int i = 0 ; i< numWorkers;i++) {
			workers.add("worker"+i);
			capacities.put("worker"+i, capacity[index++]);
			index%=capacity.length;
		}
		HashMap<String, Integer> load = new HashMap<String, Integer>();
		int numVirtualServers = 10;
		ConsistentHash<String> hash = new ConsistentHash<String>(workers, numVirtualServers);
		int numUniqueItems = 100;
		double skew = 1.0;
		ZipfDistribution zipf = new ZipfDistribution(numUniqueItems, skew);
		int numElements = 1000;
		for(int i = 0;i< numElements; i++) {
			String worker = hash.getWorker(zipf.sample());
			if(load.containsKey(worker)) {
				int count = load.get(worker);
				load.put(worker, count+1);
			}else {
				load.put(worker, 1);
			}
		}
		
		System.out.println(load);
		System.out.println(capacities);
		
	}*/

}
