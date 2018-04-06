package Partitioner;

import java.util.HashSet;

import Server.Server;
import Utility.SetFunctions;

public class VirtualWorker {
	long load;
	Server worker;
	HashSet<String> keys = new HashSet<String>();
	public VirtualWorker(long load, Server worker) { 
		this.load = load;
		this.worker= worker;
	}
	public long getLoad() {
		return load;
	}
	public void setLoad(long load) {
		this.load = load;
	}
	public Server getWorker() {
		return worker;
	}
	public void setWorker(Server worker) {
		this.worker = worker;
	}
	public void incrementNumberMessage() {
		load++;
	}
	public void addKey(String key) {
		if(!keys.contains(key))
			keys.add(key);
	}
	public HashSet<String> getKeys() {
		return keys;
	}
	public int getIntersection(VirtualWorker a) {
		return SetFunctions.intersection(this.getKeys(), a.getKeys());
	}
}
