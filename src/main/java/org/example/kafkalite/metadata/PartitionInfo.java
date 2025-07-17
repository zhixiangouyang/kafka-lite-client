package org.example.kafkalite.metadata;

public class PartitionInfo {
    private final int partitionId;
    private final int leadId;

    public PartitionInfo(int partitionId, int leadId) {
        this.partitionId = partitionId;
        this.leadId = leadId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getLeadId() {
        return leadId;
    }
}
