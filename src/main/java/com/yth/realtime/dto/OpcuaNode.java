package com.yth.realtime.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OpcuaNode {
    private String groupName;
    private String nodeName;
    private String nodeId;
    private Object value;
    private String timestamp;
}