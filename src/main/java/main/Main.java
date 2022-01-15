package main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import input.StreamItem;
import input.StreamItemReader;
import partitioner.ConsistentGroupingCH;
import partitioner.ConsistentGroupingKG;
import partitioner.ConsistentGroupingPKG;
import partitioner.ConsistentGroupingPoRC;
import partitioner.ConsistentGroupingPoTC;
import partitioner.ConsistentGroupingSG;
import partitioner.ConsistentHashing;
import partitioner.FieldGrouping;
import partitioner.LoadBalancer;
import partitioner.PartialKeyGrouping;
import partitioner.PowerofTwoChoices;
import partitioner.ShuffleGrouping;
import server.Server;
import server.TimeGranularity;
import utility.InputConfig;

public class Main {
    private static final double PRINT_INTERVAL = 1e6;

    private static void ErrorMessage() {
        String errorMessage = "Input parameters missing!!!!!!\n" + "<simulatorType>\t<input-filename>\t<output-dir>\t<num-servers>\t" + "<initial-time>\t<num-sources>\t<num-replicas>\t<epsilon>";

        System.out.println(errorMessage);

        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            ErrorMessage();
        }

        InputConfig config = new InputConfig();

        JCommander.newBuilder().addObject(config).build().parse(args);

        final int simulatorType = config.getSimulatorType();
        final String inFileName = config.getInFileName();
        final String outDir = config.getOutDir();
        final int numServers = config.getNumServers();
        final long initialTime = config.getInitialTime();
        int numSources = config.getNumSources();
        int numReplicas = config.getNumReplicas(); // consistent hashing parameter
        double epsilon = config.getEpsilon();

        long[] snapshot = new long[numServers];
        Arrays.fill(snapshot, initialTime);

        HashMap<TimeGranularity, long[]> loadMap = new HashMap<>();
        for (TimeGranularity tg : TimeGranularity.values()) {
            long load[] = new long[numServers];
            loadMap.put(tg, load);
        }

        int[] capacity = new int[numServers];
        Arrays.fill(capacity, 600);

        // initialize numServers Servers per TimeGranularity
        EnumMap<TimeGranularity, List<Server>> timeSeries = new EnumMap<TimeGranularity, List<Server>>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            List<Server> list = new ArrayList<>(numServers);
            for (int i = 0; i < numServers; i++) {
                double serverCapacity = capacity[i];
                list.add(new Server(i, initialTime, tg, 1, (int) Math.ceil(serverCapacity)));
                timeSeries.put(tg, list);
                System.out.println(serverCapacity);
            }
        }


        // initialize one output file per TimeGranularity
        EnumMap<TimeGranularity, BufferedWriter> outputs = new EnumMap<>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            outputs.put(tg, new BufferedWriter(new FileWriter(outDir + "_" + tg.toString() + ".txt")));
        }

        // initialize one LoadBalancer per TimeGranularity for simulatorTypes
        EnumMap<TimeGranularity, LoadBalancer> hashes = new EnumMap<>(TimeGranularity.class);
        for (TimeGranularity tg : TimeGranularity.values()) {
            if (simulatorType == 1) {
                hashes.put(tg, new FieldGrouping(timeSeries.get(tg)));
            } else if (simulatorType == 2) {
                hashes.put(tg, new ConsistentHashing(timeSeries.get(tg), numSources, numReplicas));
            } else if (simulatorType == 3) {
                hashes.put(tg, new PartialKeyGrouping(timeSeries.get(tg), numSources));
            } else if (simulatorType == 4) {
                hashes.put(tg, new ShuffleGrouping(timeSeries.get(tg)));
            } else if (simulatorType == 5) {
                hashes.put(tg, new ConsistentGroupingKG(timeSeries.get(tg), numSources, numReplicas));
            } else if (simulatorType == 6) {
                hashes.put(tg, new ConsistentGroupingPKG(timeSeries.get(tg), numSources, numReplicas));
            } else if (simulatorType == 7) {
                hashes.put(tg, new ConsistentGroupingPoTC(timeSeries.get(tg), numSources, numReplicas));
            } else if (simulatorType == 8) {
                hashes.put(tg, new ConsistentGroupingSG(timeSeries.get(tg), numSources, numReplicas));
            } else if (simulatorType == 9) {
                hashes.put(tg, new ConsistentGroupingPoRC(timeSeries.get(tg), numSources, numReplicas, epsilon));
            } else if (simulatorType == 10) {
                hashes.put(tg, new ConsistentGroupingCH(timeSeries.get(tg), numSources, numReplicas, epsilon));
            } else if (simulatorType == 11) {
                hashes.put(tg, new PowerofTwoChoices(timeSeries.get(tg), numSources));
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
                System.out.println("Read " + itemCount / 1000000 + "M tweets.\tSimulation time: " + (System.currentTimeMillis() - simulationStartTime) / 1000 + " seconds");
                for (BufferedWriter bw : outputs.values())
                    // flush output every PRINT_INTERVAL items
                    bw.flush();
            }

            for (StreamItem<String> item : items) {
                currentTimestamp = item.getTimestamp();
                EnumSet<TimeGranularity> statsToConsume = EnumSet.noneOf(TimeGranularity.class); // empty set of time series

                for (Entry<TimeGranularity, LoadBalancer> entry : hashes.entrySet()) {
                    LoadBalancer loadBalancer = entry.getValue();

                    Server server = loadBalancer.getServer(currentTimestamp, item);

                    loadMap.get(entry.getKey())[server.getServerID()]++;

                    boolean hasStatsReady = server.process(item.getTimestamp(), item);

                    if (hasStatsReady) statsToConsume.add(entry.getKey());

                }

                for (TimeGranularity key : statsToConsume) {
                    printStatsToConsume(timeSeries.get(key), outputs.get(key), currentTimestamp);
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
            BufferedWriter bw = new BufferedWriter(new FileWriter(outDir + "_" + tg.toString() + "_" + "summary.txt"));

            long[] load = loadMap.get(tg);
            double maxTaskLoad = Long.MIN_VALUE;
            double minTaskLoad = Long.MAX_VALUE;
            double averageTaskLoad = 0;
            for (int i = 0; i < load.length; i++) {
                double serverTaskLoad = load[i] / (double) (capacity[i] * tg.getNumberOfSeconds());
                bw.write(serverTaskLoad + "\t");
                if (maxTaskLoad < serverTaskLoad) maxTaskLoad = serverTaskLoad;
                if (minTaskLoad > serverTaskLoad) minTaskLoad = serverTaskLoad;
                averageTaskLoad += serverTaskLoad;

            }
            bw.newLine();
            double average = averageTaskLoad / numServers;
            bw.write("Max Task Load:" + maxTaskLoad + "\n");
            bw.write("Min Task Load:" + minTaskLoad + "\n");
            bw.write("Avg Task Load:" + average + "\n");

            bw.close();

        }
    }

    private static void flush(Iterable<Server> series, BufferedWriter out, long timestamp) throws IOException {
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

    private static void printStatsToConsume(Iterable<Server> servers, BufferedWriter out, long timestamp) throws IOException {
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
