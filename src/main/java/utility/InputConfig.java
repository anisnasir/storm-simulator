package utility;

import java.util.Objects;

public class InputConfig {
    int simulatorType;
    String inFileName;
    String outDir;
    int numServers;
    long initialTime;
    int numSources;
    int numReplicas;// consistent hashing parameter
    double epsilon;

    public InputConfig(String[] args) {
        simulatorType = Integer.parseInt(args[0]);
        inFileName = args[1];
        outDir = args[2];
        numServers = Integer.parseInt(args[3]);
        initialTime = Long.parseLong(args[4]);
        numSources = Integer.parseInt(args[5]);
        numReplicas = Integer.parseInt(args[6]); // consistent hashing parameter
        epsilon = Double.parseDouble(args[7]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputConfig that = (InputConfig) o;
        return simulatorType == that.simulatorType && numServers == that.numServers && initialTime == that.initialTime && numSources == that.numSources && numReplicas == that.numReplicas && Double.compare(that.epsilon, epsilon) == 0 && Objects.equals(inFileName, that.inFileName) && Objects.equals(outDir, that.outDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(simulatorType, inFileName, outDir, numServers, initialTime, numSources, numReplicas, epsilon);
    }

    public int getSimulatorType() {
        return simulatorType;
    }

    public void setSimulatorType(int simulatorType) {
        this.simulatorType = simulatorType;
    }

    public String getInFileName() {
        return inFileName;
    }

    public void setInFileName(String inFileName) {
        this.inFileName = inFileName;
    }

    public String getOutDir() {
        return outDir;
    }

    public void setOutDir(String outDir) {
        this.outDir = outDir;
    }

    public int getNumServers() {
        return numServers;
    }

    public void setNumServers(int numServers) {
        this.numServers = numServers;
    }

    public long getInitialTime() {
        return initialTime;
    }

    public void setInitialTime(long initialTime) {
        this.initialTime = initialTime;
    }

    public int getNumSources() {
        return numSources;
    }

    public void setNumSources(int numSources) {
        this.numSources = numSources;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(int numReplicas) {
        this.numReplicas = numReplicas;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }
}
