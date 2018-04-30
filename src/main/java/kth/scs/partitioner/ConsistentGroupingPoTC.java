package kth.scs.partitioner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public class ConsistentGroupingPoTC implements LoadBalancer{
	private final int numberOfReplicas;
	private final int numServers;
	List<VirtualWorker> bins;
	HashMap<Server, LinkedList<Integer>> serverBin;
	Queue<Server> underloaded;
	Queue<Server> overloaded;
	Random rand;
	
	public ConsistentGroupingPoTC(Collection<Server> nodes, int numberOfSources, int numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
		this.numServers = nodes.size();
		this.bins = new ArrayList<VirtualWorker>();
		this.underloaded = new LinkedList<Server>();
		this.overloaded = new LinkedList<Server>();
		this.serverBin = new HashMap<Server, LinkedList<Integer>> ();
		this.rand = new Random();
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
		
		int firstChoice = rand.nextInt(bins.size());
		int secondChoice = rand.nextInt(bins.size());
		
		VirtualWorker firstBin = bins.get(firstChoice);
		VirtualWorker secondBin = bins.get(secondChoice);
		
		VirtualWorker selected = firstBin.getLoad() < secondBin.getLoad() ? firstBin : secondBin;
		selected.incrementNumberMessage();
		
		return selected.getWorker();
		
	} 


}