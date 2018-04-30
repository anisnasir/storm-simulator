package kth.scs.partitioner;

import java.util.List;
import java.util.Random;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public class PowerofTwoChoices implements LoadBalancer {
	private final List<Server> nodes;
	private final int serversNo;
	private final int numSources;
	private long[] localworkload[];
	private int sourceCount;
	Random rand = new Random();

	public PowerofTwoChoices(List<Server> nodes, int numSources) {
		this.nodes = nodes;
		this.numSources = numSources;
		this.serversNo = nodes.size();
		this.localworkload = new long[numSources][];
		for (int i = 0; i < numSources; i++)
			localworkload[i] = new long[nodes.size()];
		this.sourceCount = 0;
	}

	public Server getServer(long timestamp, StreamItem item) {
		int source = (this.sourceCount++) % this.numSources;
		this.sourceCount %= this.numSources;

		Object key = item.getTaskID();
		byte b[] = key.toString().getBytes();

		int firstChoice = rand.nextInt(serversNo);
		int secondChoice = rand.nextInt(serversNo);
		int selected = localworkload[source][firstChoice] > localworkload[source][secondChoice] ? secondChoice : firstChoice;

		Server selectedNode = nodes.get(selected);
		localworkload[source][selected]++;
		return selectedNode;
	}
}
