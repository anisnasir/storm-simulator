package Partitioner;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import Input.StreamItem;
import Server.Server;

public class FieldGrouping implements LoadBalancer {
	private final SortedMap<Integer, Server> circle = new TreeMap<Integer, Server>();
	private int numServers;
	HashFunction h1 = Hashing.murmur3_128(13);

	public FieldGrouping(Collection<Server> nodes) {
		this.numServers = nodes.size();
		int i = 0;
		for (Server node : nodes) {
			circle.put(i, node);
			i++;
		}
	}

	public Server getServer(long timestamp, StreamItem item) {
		//int serverID = Math.abs(key.toString().hashCode()) % this.numServers;
		//Seed seeds = new Seed(this.numServers);
		Object key = item.getTaskID();
		
		int serverID = Math.abs(h1.hashBytes(key.toString().getBytes()).asInt()%numServers); 
		
		return this.circle.get(serverID);
	}

}
