package partitioner;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import input.StreamItem;
import server.Server;


public class ShuffleGrouping implements LoadBalancer{
	private final int numWorkers; 
	private int currentServer;
	private final SortedMap<Integer, Server> circle = new TreeMap<Integer, Server>();
	
	public ShuffleGrouping(Collection<Server> nodes) {
		this.numWorkers = nodes.size();
		this.currentServer = 0;
		
		int i = 0;
		for (Server node : nodes) {
			circle.put(i, node);
			i++;
		}
		
	}
	@Override
	public Server getServer(long timestamp, StreamItem item) {
		Server server = circle.get(currentServer);
		currentServer++;
		currentServer%=this.numWorkers;
		
		return server;
	}


}