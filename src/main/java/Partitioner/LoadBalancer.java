package Partitioner;

import Input.StreamItem;
import Server.Server;

public interface LoadBalancer {
	public Server getServer(long timestamp, StreamItem item);
}
