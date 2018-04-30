package kth.scs.partitioner;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;

import kth.scs.hashing.Hasher;
import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public class ConsistentHashing implements LoadBalancer{
	private final Hasher hashFunction;
	private final int numberOfReplicas;
	private final SortedMap<Integer, Server> circle =
			new TreeMap<Integer, Server>();
	private final HashMap<Server, Integer> replicaCount = 
			new HashMap<Server, Integer>(); 
	
	
	public ConsistentHashing(Collection<Server> nodes,
			int numberOfSources, int numberOfReplicas) {

		this.hashFunction = new Hasher(13);
		this.numberOfReplicas = numberOfReplicas;
		for (Server node : nodes) {
			add(node);
		}
	}

	public void add(Server node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(hashFunction.hash(node.toString() +":"+ i),
					node);		
		}
		replicaCount.put(node, numberOfReplicas);
		
	}
	
	public void increaseLoad(Server node) {
		int  count = replicaCount.get(node);
		circle.put(hashFunction.hash(node.toString()+":"+(count)),node);
		replicaCount.put(node, count+1);
		
	}
	
	public void reduceLoad(Server node) {
		int  replica = replicaCount.get(node);
		if(replica <= 1)
			return;
		circle.remove(hashFunction.hash(node.toString()+":"+(replica-1)));
		replicaCount.put(node, replica-1);
		
	}
	public void remove(Server node) {
		int count = replicaCount.get(node);
		for (int i = 0; i < count; i++) {
			circle.remove(hashFunction.hash(node.toString()+":" + i));
		}
	}

	public Server getServer(long timestamp, StreamItem item) {
		Object key = item.getTaskID();
		if (circle.isEmpty()) {
			return null;
		}
		int hash = hashFunction.hash(key.toString());
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, Server> tailMap =
					circle.tailMap(hash);
			hash = tailMap.isEmpty() ?
					circle.firstKey() : tailMap.firstKey();
		}
		
		
		Server server = circle.get(hash);
		return server;
	} 


}