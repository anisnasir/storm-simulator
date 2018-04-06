package Server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import Input.StreamItem;

/**
 * Statistics for a server.
 * 
 */
public class ServerStats implements Serializable {
	private static final long serialVersionUID = 1L;
	long finishedTasks;
	long totalExecuteLatency;
	long capacity;
	long totalProcessed;
	private Queue<StreamItem<String>> inputQueue;
	
	public ServerStats(long capacity, Queue<StreamItem<String>> inputQueue) {
		this.finishedTasks = 0 ;
		this.capacity = capacity;
		this.inputQueue = inputQueue;
	}
	public void addTask(StreamItem<String> task) {
		totalProcessed+= task.getProcessingTime();
		finishedTasks++;
		//System.out.println(totalExecuteLatency + " " + finishedTasks);
		totalExecuteLatency += task.getExecuteLatency();
	}
	
	public long getCapacity() { 
		return this.capacity;
	}
	public long getNumFinishedTasks() {
		return this.finishedTasks;
	}
	public long getTotalProcessed() { 
		return this.totalProcessed;
	}
	
	public double getAverageExecuteLatency() {
		double value = totalExecuteLatency/(double)finishedTasks;
		//System.out.println(value);
		return value;
	}
	
	public String toString() {
		return inputQueue.size() + " " + finishedTasks+ " " + getAverageExecuteLatency() + " " + totalProcessed +" " + getResourceUtilization()+ " " +(inputQueue.size()+finishedTasks)+ "\t";
	}
	public int compareTo(ServerStats o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public double getResourceUtilization() {
		//System.out.println(totalProcessed+" " + capacity);
		return this.totalProcessed/(double)this.capacity;
	}
}
