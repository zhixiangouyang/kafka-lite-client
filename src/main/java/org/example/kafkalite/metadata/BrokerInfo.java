package org.example.kafkalite.metadata;

public class BrokerInfo {
    private final int nodeId;
    private final int port;
    private final String host;

    public BrokerInfo(int nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.host = host;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public String toAddress() {
        return host + ":" + port;
    }
}
