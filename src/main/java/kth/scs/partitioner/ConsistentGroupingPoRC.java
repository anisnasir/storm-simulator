package kth.scs.partitioner;
import java.util.Collection;
import java.util.HashMap;

import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public class ConsistentGroupingPoRC implements LoadBalancer{
	int numSources;
	int prevSource;
	HashMap<Integer, ConsistentGroupingPoRCSingleSource> map;
	
	public ConsistentGroupingPoRC(Collection<Server> nodes, int numberOfSources, int numberOfReplicas, double epsilon) {
		
		this.numSources = numberOfSources;
		map = new HashMap<Integer, ConsistentGroupingPoRCSingleSource>();
		
		for(int i = 0 ; i< this.numSources;i++) {
			map.put(i, new ConsistentGroupingPoRCSingleSource(nodes, numberOfReplicas, epsilon));
		}
	}
	
	public void increaseLoad(int source, Server underloadedWorker) {
		map.get(source).increaseLoad(underloadedWorker);
	}
	
	public void reduceLoad(int source, Server overloadedWorker) {
		map.get(source).reduceLoad(overloadedWorker);
	}
	
	public Server getServer(long timestamp, StreamItem item) {
		Server temp = map.get(prevSource++).getServer(timestamp, item);
		prevSource%=numSources;
		return temp;
	} 


}