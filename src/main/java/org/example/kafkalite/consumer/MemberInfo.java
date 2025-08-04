package org.example.kafkalite.consumer;

import java.util.List;

public class MemberInfo {
    private final String memberId;
    private final List<String> subscribedTopics;
    
    public MemberInfo(String memberId, List<String> subscribedTopics) {
        this.memberId = memberId;
        this.subscribedTopics = subscribedTopics;
    }
    
    public String getMemberId() {
        return memberId;
    }
    
    public List<String> getSubscribedTopics() {
        return subscribedTopics;
    }
    
    @Override
    public String toString() {
        return String.format("MemberInfo(memberId=%s, topics=%s)", memberId, subscribedTopics);
    }
} 