package utility;

import com.beust.jcommander.Parameter;

import java.util.Objects;

public class InputConfig {
    @Parameter(names = "-simulatorType", description = "provide the simulator type")
    int simulatorType;

    @Parameter(names = "-input", description = "input file name")
    String inFileName;

    @Parameter(names = "-outputDir", description = "output directory")
    String outDir;

    @Parameter(names = "-numServers", description = "number of servers")
    int numServers;

    @Parameter(names = "-initialTime", description = "initial timestamp for the stream")
    long initialTime;

    @Override
    public String toString() {
        return "InputConfig{" +
                "simulatorType=" + simulatorType +
                ", inFileName='" + inFileName + '\'' +
                ", outDir='" + outDir + '\'' +
                ", numServers=" + numServers +
                ", initialTime=" + initialTime +
                ", numSources=" + numSources +
                ", numReplicas=" + numReplicas +
                ", epsilon=" + epsilon +
                '}';
    }

    @Parameter(names = "-numSources", description = "number of sources")
    int numSources;

    @Parameter(names = "-numReplicas", description = "consistent hashing parameter")
    int numReplicas;

    @Parameter(names = "-epsilon", description = "epsilon")
    double epsilon;

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
