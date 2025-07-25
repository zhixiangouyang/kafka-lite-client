package org.example.kafkalite.metadata;

public class PartitionInfo {
    private final int partitionId;
    private final String leaderAddress;

    public PartitionInfo(int partitionId, String leaderAddress) {
        this.partitionId = partitionId;
        this.leaderAddress = leaderAddress;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }
    
    @Override
    public String toString() {
        return "PartitionInfo{" +
                "partitionId=" + partitionId +
                ", leaderAddress='" + leaderAddress + '\'' +
                '}';
    }
}
