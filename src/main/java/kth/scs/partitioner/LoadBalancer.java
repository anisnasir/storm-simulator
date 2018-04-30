package kth.scs.partitioner;

import kth.scs.input.StreamItem;
import kth.scs.server.Server;

public interface LoadBalancer {
	public Server getServer(long timestamp, StreamItem item);
}
