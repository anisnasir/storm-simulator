package server;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Writer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

import input.StreamItem;

/**
 * Represents a single server
 */
public class Server {
	private long currentTimestamp;
	private final long granularity;
	private final int minSize;
	private long capacity; // capacity is the number of cycles in the window (granularity)
	private long currentCapacity; // remaining capacity is the number of cycles in the window (granularity)
	private Queue<StreamItem<String>> inputQueue;
	long unProcessed = 0;
	private final Deque<ServerStats> timeSeries = new LinkedList<>();
	ServerStats lastStats = null;
	int serverID;

	public Server(int id, long initialTimestamp, TimeGranularity granularity,
			int minSize, long capacity) {
		this.serverID = id;
		this.currentTimestamp = initialTimestamp;
		this.granularity = granularity.getNumberOfSeconds();
		this.minSize = (minSize > 0) ? minSize : 1; // minimum size = 1
		this.capacity = capacity;
		this.currentCapacity = capacity;
		//System.out.println(capacity);
		this.inputQueue = new LinkedList<StreamItem<String>>();
		this.timeSeries.add(new ServerStats(capacity, this.inputQueue));
	}
	public void updateCapacity(long capacity) {
		this.capacity = capacity;
	}

	public boolean process(long timestamp, StreamItem item) {
		inputQueue.add(item);
		unProcessed+=item.getProcessingTime();
		synch(timestamp);	
		processQueue(timestamp);
		return timeSeries.size() > minSize;
	}
	public void synch(long newTimestamp) {
		// add new elements to the time serie until we get one for the right
		// windows
		while (this.currentTimestamp + this.granularity - 1 < newTimestamp) {
			ServerStats newStats = new ServerStats(capacity, this.inputQueue);
			this.timeSeries.addLast(newStats);
			this.currentTimestamp += this.granularity;
			this.currentCapacity = this.capacity;
			processQueue(newTimestamp);
		}
	}
	
	
	private void processQueue(long currentTime) {
		while(!inputQueue.isEmpty()) {
			StreamItem<String> item = inputQueue.peek();
			if(item.getTimestamp() + item.getProcessingTime() <= currentTimestamp && item.getProcessingTime() < currentCapacity) {
				item = inputQueue.poll();
				unProcessed-=item.getProcessingTime();
				//Finished time = max(currentTime, arrivalTime) + processingTime + queueingTime;
				item.setFinishedTime( Math.max( (this.currentTimestamp - this.granularity), item.getTimestamp()) +  item.getProcessingTime()+(capacity-currentCapacity));
				//System.out.println(" execute latency" + (item.getFinishedTime()-item.getTimestamp()));
				timeSeries.peekFirst().addTask(item);
				currentCapacity-= item.getProcessingTime();
			} else 
				break;
		}
	}
	
	

	public boolean flushNext(Writer out) {
		checkArgument(!timeSeries.isEmpty());
		if (timeSeries.isEmpty())
			return false;
		try {
			//System.out.println(timeSeries.peekFirst().finishedTasks);
			lastStats = timeSeries.pollFirst();
			out.write(lastStats.toString());			
			return !timeSeries.isEmpty();
		} catch (IOException e) {
			System.err.println("Problem writing time serie to output file");
			e.printStackTrace();
			return false;
		}
	}
	public long getGranularity() {
		return granularity;
	}

	public boolean printNextUnused(Writer out) {
		if (timeSeries.size() <= minSize)
			return false;
		try {
			checkArgument(!timeSeries.isEmpty());
			lastStats = timeSeries.pollFirst();
			out.write(lastStats.toString());
			return (timeSeries.size() > minSize);
		} catch (IOException e) {
			System.err.println("Problem writing time serie to output file");
			e.printStackTrace();
			return false;
		}
	}

	public String getLocalState() { 
		return this.currentTimestamp + " " + inputQueue.size() + " " + timeSeries.size() +  " ";
	}


	public int getStatsSize() {
		return timeSeries.size();
	}

	public int getQueueSize() {
		return this.inputQueue.size();
	}
	public int getServerID() {
		return this.serverID;
	}
	public long getCapacity() { 
		return this.capacity;
	}
	public double getResourceUtilization() {
		if(lastStats == null)
			return 0.0;
		else 
			return lastStats.getResourceUtilization();
	}
	public double getStats() { 
		return unProcessed/(double)(capacity);
	}
	
}
