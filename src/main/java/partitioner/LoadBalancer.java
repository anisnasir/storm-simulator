package partitioner;

import input.StreamItem;
import server.Server;

public interface LoadBalancer {
	public Server getServer(long timestamp, StreamItem item);
}
