package org.example.kafkalite.metadata;

import java.util.Map;

public interface MetadataManager {
    void refreshMetadata(String topic);
    Map<Integer, String> getPartitionLeaders(String topic);
}
