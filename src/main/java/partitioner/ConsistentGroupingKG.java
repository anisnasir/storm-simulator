package partitioner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import hashing.Hasher;
import input.StreamItem;
import server.Server;

public class ConsistentGroupingKG implements LoadBalancer{
	private final int numberOfReplicas;
	private final int numServers;
	List<VirtualWorker> bins;
	HashMap<Server, LinkedList<Integer>> serverBin;
	Queue<Server> underloaded;
	Queue<Server> overloaded;
	Hasher hash = new Hasher(105929);
	
	
	public ConsistentGroupingKG(Collection<Server> nodes, int numberOfSources, int numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
		this.numServers = nodes.size();
		this.bins = new ArrayList<VirtualWorker>();
		this.underloaded = new LinkedList<Server>();
		this.overloaded = new LinkedList<Server>();
		this.serverBin = new HashMap<Server, LinkedList<Integer>> ();
		for (Server node : nodes) {
			add(node);
		}
	}

	public void add(Server node) {
		LinkedList<Integer> temp = new LinkedList<Integer>();
		for (int i = 0; i < numberOfReplicas; i++) {
			temp.add(bins.size());
			bins.add(new VirtualWorker(0,node));
		}
		serverBin.put(node, temp);
	}
	
	public void increaseLoad(Server underloadedWorker) {
		if(overloaded.isEmpty()) {
			underloaded.add(underloadedWorker);
		}else {
			Server overloadedWorker = overloaded.poll();
			LinkedList<Integer> overLoadedBins = serverBin.get(overloadedWorker);
			int binIndex = overLoadedBins.getLast();
			overLoadedBins.removeLast();
			LinkedList<Integer> underLoadedBins = serverBin.get(underloadedWorker);
			underLoadedBins.add(binIndex);
			bins.get(binIndex).setWorker(underloadedWorker);
			
		}
		
	}
	
	public void reduceLoad(Server overloadedWorker) {
		if(underloaded.isEmpty()) {
			overloaded.add(overloadedWorker);
		}else {
			Server underloadedWorker = overloaded.poll();
			LinkedList<Integer> overLoadedBins = serverBin.get(overloadedWorker);
			int binIndex = overLoadedBins.getLast();
			overLoadedBins.removeLast();
			LinkedList<Integer> underLoadedBins = serverBin.get(underloadedWorker);
			underLoadedBins.add(binIndex);
			bins.get(binIndex).setWorker(underloadedWorker);
			
		}
	}
	
	@Override
	public Server getServer(long timestamp, StreamItem item) {
		Object key = item.getTaskID();
		int firstChoice = hash.hash(key.toString())%bins.size();
		VirtualWorker selected = bins.get(firstChoice);
		selected.incrementNumberMessage();
		return selected.getWorker();
		
	} 


}