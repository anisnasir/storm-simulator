package kth.scs.partitioner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import kth.scs.hashing.Hasher;
import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public class ConsistentGroupingCH implements LoadBalancer{
	private final int numberOfReplicas;
	List<VirtualWorker> bins;
	HashMap<Server, LinkedList<Integer>> serverBin;
	Queue<Server> underloaded;
	Queue<Server> overloaded;
	int numMessages = 0;
	double epsilon = 0.3;
	Hasher hash1 = new Hasher(105929);
	
	public ConsistentGroupingCH(Collection<Server> nodes, int numberOfSources, int numberOfReplicas, double epsilon) {
		this.numberOfReplicas = numberOfReplicas;
		this.bins = new ArrayList<VirtualWorker>();
		this.underloaded = new LinkedList<Server>();
		this.overloaded = new LinkedList<Server>();
		this.serverBin = new HashMap<Server, LinkedList<Integer>> ();
		for (Server node : nodes) {
			add(node);
		}
		this.epsilon = epsilon;
	}

	public void add(Server node) {
		LinkedList<Integer> temp = new LinkedList<Integer>();
		for (int i = 0; i < numberOfReplicas; i++) {
			temp.add(bins.size());
			bins.add(new VirtualWorker(0,node));
		}
		serverBin.put(node, temp);
		Collections.shuffle(bins);
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
		String key = item.getTaskID().toString();
		numMessages++;			
		double avgLoad = numMessages/(double)bins.size();	
		int candidateChoice = hash1.hash(key)%bins.size();	
		VirtualWorker candidateBin = bins.get(candidateChoice);
		while(candidateBin.getLoad() >= ((1+epsilon)*avgLoad)) {
			candidateChoice++;
			candidateChoice%=bins.size();
			candidateBin = bins.get(candidateChoice);
		}
		candidateBin.incrementNumberMessage();
		return candidateBin.getWorker();
		
	} 


}