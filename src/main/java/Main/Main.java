package Main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import Input.StreamItem;
import Input.StreamItemReader;
import Partitioner.ConsistentGroupingCH;
import Partitioner.ConsistentGroupingKG;
import Partitioner.ConsistentGroupingPKG;
import Partitioner.ConsistentGroupingPoRC;
import Partitioner.ConsistentGroupingPoTC;
import Partitioner.ConsistentGroupingSG;
import Partitioner.ConsistentHashing;
import Partitioner.FieldGrouping;
import Partitioner.PartialKeyGrouping;
import Partitioner.PowerofTwoChoices;
import Partitioner.ShuffleGrouping;
import Partitioner.LoadBalancer;
import Server.Server;
import Server.ServerStats;
import Server.TimeGranularity;

public class Main {
	private static final double PRINT_INTERVAL = 1e6;

	private static void ErrorMessage() {
		System.err.println("final int simulatorType = Integer.parseInt(args[0]);\n" + 
				"		final String inFileName = args[1];\n" + 
				"		final String outDir = args[2];\n" + 
				"		final int numServers = Integer.parseInt(args[3]);\n" + 
				"		final long initialTime = Long.parseLong(args[4]);\n" + 
				"		int numSources = Integer.parseInt(args[5]); \n" + 
				"		int numReplicas = Integer.parseInt(args[6]); // consistent hashing parameter\n" + 
				"		double epsilon = Double.parseDouble(args[7]);");

		System.exit(1);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 5) {
			ErrorMessage();
		}

		final int simulatorType = Integer.parseInt(args[0]);
		final String inFileName = args[1];
		final String outDir = args[2];
		final int numServers = Integer.parseInt(args[3]);
		final long initialTime = Long.parseLong(args[4]);
		int numSources = Integer.parseInt(args[5]); 
		int numReplicas = Integer.parseInt(args[6]); // consistent hashing parameter
		double epsilon = Double.parseDouble(args[7]);
		long [] snapshot = new long[numServers];
		Arrays.fill(snapshot, initialTime);


		HashMap<TimeGranularity, long[]> loadMap = new HashMap<TimeGranularity, long[]>();
		//HashMap<TimeGranularity, ArrayList<HashSet<String>>> memoryMap = new HashMap<TimeGranularity,  ArrayList<HashSet<String>>>();
		for (TimeGranularity tg : TimeGranularity.values()) {
			long load[] = new long[numServers];
			loadMap.put(tg, load);

			/*ArrayList<HashSet<String>> list = new ArrayList<HashSet<String>>();
			memoryMap.put(tg, list);
			for(int i = 0 ;i <numServers;i++) {
				HashSet<String> temp = new HashSet<String>();
				memoryMap.get(tg).add(temp);
			}*/

		}


		/*int y = 3;
		int z = 5;
		int totalCapacity = 25*60*numServers;
		double a = totalCapacity/(double)(numServers-y+y*z);
		 */


		/*int [] capacity = new int[numServers];
		Arrays.fill(capacity, 1);
		for(int i= 0; i<y;i++) {
			capacity[i] = z;
		}
		 */
		int [] capacity = new int[numServers];
		Arrays.fill(capacity, 600);

		// initialize numServers Servers per TimeGranularity
		EnumMap<TimeGranularity, List<Server>> timeSeries = new EnumMap<TimeGranularity, List<Server>>(
				TimeGranularity.class);
		for (TimeGranularity tg : TimeGranularity.values()) {
			List<Server> list = new ArrayList<Server>(numServers);
			for (int i = 0; i < numServers; i++) {
				double serverCapacity = capacity[i];
				list.add(new Server(i, initialTime, tg, 1 , (int)Math.ceil(serverCapacity)));
				timeSeries.put(tg, list);
				System.out.println(serverCapacity);
			}
		}


		// initialize one output file per TimeGranularity
		EnumMap<TimeGranularity, BufferedWriter> outputs = new EnumMap<TimeGranularity, BufferedWriter>(
				TimeGranularity.class);
		for (TimeGranularity tg : TimeGranularity.values()) {
			outputs.put(tg, new BufferedWriter(new FileWriter(outDir+"_"
					+ tg.toString()+".txt")));
		}

		// initialize one LoadBalancer per TimeGranularity for simulatorTypes
		EnumMap<TimeGranularity, LoadBalancer> hashes = new EnumMap<TimeGranularity, LoadBalancer>(
				TimeGranularity.class);
		for (TimeGranularity tg : TimeGranularity.values()) {
			if (simulatorType == 1) {
				hashes.put(tg, new FieldGrouping(timeSeries.get(tg)));	
			} else if (simulatorType == 2 ) {
				hashes.put(tg, new ConsistentHashing(timeSeries.get(tg),
						numSources,numReplicas));
			}else if (simulatorType == 3) {
				hashes.put(tg, new PartialKeyGrouping(timeSeries.get(tg),
						numSources));
			}else if (simulatorType == 4 ) {
				hashes.put(tg, new ShuffleGrouping(timeSeries.get(tg)));
			}else if (simulatorType == 5 ) {
				hashes.put(tg, new ConsistentGroupingKG(timeSeries.get(tg),
						numSources,numReplicas));
			} else if (simulatorType == 6 ) {
				hashes.put(tg, new ConsistentGroupingPKG(timeSeries.get(tg),
						numSources,numReplicas));
			} else if (simulatorType == 7 ) {
				hashes.put(tg, new ConsistentGroupingPoTC(timeSeries.get(tg),
						numSources,numReplicas));
			} else if (simulatorType == 8 ) {
				hashes.put(tg, new ConsistentGroupingSG(timeSeries.get(tg),
						numSources,numReplicas));
			}else if (simulatorType == 9 ) {
				hashes.put(tg, new ConsistentGroupingPoRC(timeSeries.get(tg),
						numSources,numReplicas, epsilon));
			}else if (simulatorType == 10 ) {
				hashes.put(tg, new ConsistentGroupingCH(timeSeries.get(tg),
						numSources,numReplicas, epsilon));
			}else if (simulatorType == 11) {
				hashes.put(tg, new PowerofTwoChoices(timeSeries.get(tg),
						numSources));
			}


		}

		// read items and route them to the correct server
		System.out.println("Starting to read the item stream");


		// core loop
		long simulationStartTime = System.currentTimeMillis();
		StreamItemReader reader = new StreamItemReader(inFileName);
		List<StreamItem<String>> items = reader.nextItem();
		long currentTimestamp = 0;
		int itemCount = 0;

		while (items != null) {

			if (++itemCount % PRINT_INTERVAL == 0) {
				System.out.println("Read " + itemCount / 1000000
						+ "M tweets.\tSimulation time: "
						+ (System.currentTimeMillis() - simulationStartTime)
						/ 1000 + " seconds");
				for (BufferedWriter bw : outputs.values())
					// flush output every PRINT_INTERVAL items
					bw.flush();
			}

			/*if(itemCount == 6000000) {
				y = 5;
				z = 4;
				totalCapacity = 25*60*numServers;
				a = totalCapacity/(double)(numServers-y+y*z);
				capacity = new int[numServers];
				Arrays.fill(capacity, 1);
				for(int i= 0; i<y;i++) {
					capacity[i] = z;
				}
				for (TimeGranularity tg : TimeGranularity.values()) {
					for (int i = 0; i < numServers; i++) {
						double serverCapacity = capacity[i]*a;
						List<Server> list = timeSeries.get(tg);
						Server server = list.get(i);
						server.updateCapacity((long)serverCapacity);
					}
				}
			}
			if(itemCount == 12000000) {
				y = 2;
				z = 10;
				totalCapacity = 25*60*numServers;
				a = totalCapacity/(double)(numServers-y+y*z);
				capacity = new int[numServers];
				Arrays.fill(capacity, 1);
				for(int i= 0; i<y;i++) {
					capacity[i] = z;
				}
				for (TimeGranularity tg : TimeGranularity.values()) {
					for (int i = 0; i < numServers; i++) {
						double serverCapacity = capacity[i]*a;
						List<Server> list = timeSeries.get(tg);
						Server server = list.get(i);
						server.updateCapacity((long)serverCapacity);
					}
				}
			}
			 */
			for(StreamItem<String> item: items) {
				currentTimestamp = item.getTimestamp();
				EnumSet<TimeGranularity> statsToConsume = EnumSet
						.noneOf(TimeGranularity.class); // empty set of time series

				for (Entry<TimeGranularity, LoadBalancer> entry : hashes
						.entrySet()) {
					LoadBalancer loadBalancer = entry.getValue();

					Server server = loadBalancer.getServer(currentTimestamp,
							item);

					/*if(simulatorType == 9) {
						if(snapshot[server.getServerID()] + 60 < currentTimestamp) {
							double resourceUtilization = server.getResourceUtilization();
							//System.out.println(resourceUtilization);
							if(resourceUtilization == 0.0) {

							}
							else if(resourceUtilization > 0.85) {
								//System.out.println("reducing workload");
								for(int i = 0 ; i< numSources;i++) {
									ConsistentGroupingPoRC temp = (ConsistentGroupingPoRC) loadBalancer;
									temp.reduceLoad(i, server);
									snapshot[server.getServerID()] = currentTimestamp;
								}
							}
							else if(resourceUtilization < 0.75 ) {
								//System.out.println("increasing workload");
								for(int i = 0 ; i< numSources;i++) {
									ConsistentGroupingPoRC temp = (ConsistentGroupingPoRC) loadBalancer;
									temp.increaseLoad(i, server);
									snapshot[server.getServerID()] = currentTimestamp;
								}
							}
						}

					}*/

					loadMap.get(entry.getKey())[server.getServerID()]++;
					//memoryMap.get(entry.getKey()).get(server.getServerID()).add(item.getTaskID());

					boolean hasStatsReady = server.process(item.getTimestamp(),item);

					if (hasStatsReady)
						statsToConsume.add(entry.getKey());

				}

				for (TimeGranularity key : statsToConsume) {
					printStatsToConsume(timeSeries.get(key), outputs.get(key),
							currentTimestamp);
				}
			}
			items = reader.nextItem();
		}
		// print final stats
		for (TimeGranularity tg : TimeGranularity.values()) {
			flush(timeSeries.get(tg), outputs.get(tg), currentTimestamp);
		}

		// close all files
		reader.close();
		for (BufferedWriter bw : outputs.values())
			bw.close();

		System.out.println("Finished reading items\nTotal items: " + itemCount);

		for (TimeGranularity tg : TimeGranularity.values()) {
			BufferedWriter bw = new BufferedWriter(new FileWriter(outDir+"_"+tg.toString()+"_"+"summary.txt"));

			long [] load = loadMap.get(tg);
			double maxTaskLoad = Long.MIN_VALUE;
			double minTaskLoad = Long.MAX_VALUE;
			double averageTaskLoad = 0 ;
			for(int i = 0; i<load.length;i++) {
				double serverTaskLoad = load[i]/(double)(capacity[i]*tg.getNumberOfSeconds());
				bw.write(serverTaskLoad+ "\t");
				if(maxTaskLoad < serverTaskLoad)
					maxTaskLoad = serverTaskLoad;
				if(minTaskLoad > serverTaskLoad)
					minTaskLoad = serverTaskLoad;
				averageTaskLoad+= serverTaskLoad;

			}
			bw.newLine();
			double average = averageTaskLoad/(double)numServers;
			bw.write("Max Task Load:" + maxTaskLoad+"\n");
			bw.write("Min Task Load:" + minTaskLoad+"\n");
			bw.write("Avg Task Load:" + average+"\n");


			/*ArrayList<HashSet<String>> list = memoryMap.get(tg);
			long sum = 0;
			HashSet<String> allKeys = new HashSet<String>();
			for(int j=0;j<list.size();j++) {
				HashSet<String> temp  =  list.get(j);
				sum+= temp.size();
				allKeys.addAll(temp);
			}

			double memoryCost = sum/(double)allKeys.size();
			bw.write("Memory Cost:" + memoryCost+"\n");*/
			bw.close();
			 
		}
	}

	private static void flush(Iterable<Server> series, BufferedWriter out,
			long timestamp) throws IOException {
		for (Server serie : series) { // sync all servers to the current
			// timestamp
			serie.synch(timestamp);
		}
		boolean hasMore = false;
		do {
			for (Server serie : series) {
				hasMore &= serie.flushNext(out);
			}
			out.newLine();
		} while (hasMore);
	}

	private static void printStatsToConsume(Iterable<Server> servers,
			BufferedWriter out, long timestamp) throws IOException {
		for (Server sever : servers) { // sync all servers to the current
			// timestamp
			sever.synch(timestamp);
		}
		boolean hasMore = false;
		do {
			for (Server server : servers) { // print up to the point in which
				// all the servers have stats ready
				// (AND barrier)
				hasMore &= server.printNextUnused(out);
			}
			out.newLine();
		} while (hasMore);
	}
}
