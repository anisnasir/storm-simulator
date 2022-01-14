package input;

import java.io.Serializable;

/**
 * An atomic item in the stream. An item is composed by a timestamp and a line
 * of text. The timestamp represents the position in the stream, the text is the
 * payload and is represented as list of strings.
 */
public class StreamItem<T> implements Serializable{
	private T taskID;
	private long timestamp;
	private long processingTime;
	private long finishedTime;
	
	@Override
	public String toString() {
		return taskID.toString() + " " + timestamp;
	}
	public StreamItem(long arrivalTime, T taskID, long processingTime) {
		this.taskID = taskID;
		this.timestamp = arrivalTime;
		this.processingTime = processingTime;
	}
	
	public long getExecuteLatency() {
		if(finishedTime == 0)
			return Integer.MAX_VALUE;
		else {
			//System.out.println(finishedTime + " " + timestamp +" " + (finishedTime-timestamp));
			return finishedTime-timestamp;
		}
	}
	
	public T getTaskID() {
		return taskID;
	}
	public void setTaskID(T taskID) {
		this.taskID = taskID;
	}
	
	public long getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(long arrivalTime) {
		this.timestamp = arrivalTime;
	}


	public long getProcessingTime() {
		return processingTime;
	}


	public void setProcessingTime(long processingTime) {
		this.processingTime = processingTime;
	}
	public long getFinishedTime() {
		return finishedTime;
	}


	public void setFinishedTime(long finishedTime) {
		this.finishedTime = finishedTime;
	}

}
