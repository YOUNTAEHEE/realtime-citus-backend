package com.yth.realtime.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class OpcuaClient {
    private static final Logger log = LoggerFactory.getLogger(OpcuaClient.class);

    private OpcUaClient client;
    private boolean connected = false;
    private final Map<String, Map<String, NodeId>> groupedNodes = new HashMap<>();

    // OPC UA 서버 주소 및 노드 ID 설정
    private static final String SERVER_URL = "opc.tcp://192.168.10.35:4840";
    private static final Map<String, String> OBJECT_NODES = Map.of(
            "OPC_UA", "ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG",
            "PLC_PRG", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.PLC_PRG",
            "GVL", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.GVL",
            "HIS", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.His10ms");

    /**
     * OPC UA 서버에 연결
     */
    public boolean connect() {
        if (connected) {
            log.info("이미 OPC UA 서버에 연결되어 있습니다.");
            return true;
        }

        try {
            // 서버 엔드포인트 탐색
            List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(SERVER_URL).get();
            EndpointDescription endpoint = endpoints.stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No endpoints found"));

            // 클라이언트 구성
            OpcUaClientConfigBuilder configBuilder = new OpcUaClientConfigBuilder();
            configBuilder.setEndpoint(endpoint);

            // 클라이언트 생성 및 연결
            client = OpcUaClient.create(configBuilder.build());
            client.connect().get();
            connected = true;

            log.info("OPC UA 서버에 연결되었습니다: {}", SERVER_URL);

            // 변수 노드 초기화
            initializeNodes();

            return true;
        } catch (Exception e) {
            log.error("OPC UA 서버 연결 실패: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * OPC UA 서버와의 연결 해제
     */
    public void disconnect() {
        if (client != null && connected) {
            try {
                client.disconnect().get();
                connected = false;
                log.info("OPC UA 서버 연결이 종료되었습니다");
            } catch (Exception e) {
                log.error("OPC UA 서버 연결 해제 실패: {}", e.getMessage(), e);
            }
        }
    }

    private void initializeNodes() throws UaException {
        log.info("OPC UA 변수 노드 초기화 시작");
    
        for (Map.Entry<String, String> entry : OBJECT_NODES.entrySet()) {
            String groupName = entry.getKey();
            String nodeIdStr = entry.getValue();
    
            // 각 그룹별 노드 맵 생성
            Map<String, NodeId> nodeMap = new HashMap<>();
            groupedNodes.put(groupName, nodeMap);
    
            try {
                NodeId objectNodeId = NodeId.parse(nodeIdStr);
    
                // browseBranchNode(...) 호출 시 nodeMap을 넘겨줌
                List<NodeId> childNodeIds = browseBranchNode(objectNodeId, nodeMap);
    
                log.info("{} 그룹에서 {} 개의 노드를 찾았습니다", groupName, childNodeIds.size());
    
            } catch (Exception e) {
                log.error("{} 객체의 변수 노드 초기화 실패: {}", groupName, e.getMessage(), e);
            }
        }
    
        log.info("모든 변수 노드 초기화 완료");
    }
    
    /**
     * 특정 노드의 자식 노드들을 찾아, (browseName -> NodeId) 매핑하고 리스트로 반환
     */
    private List<NodeId> browseBranchNode(NodeId nodeId, Map<String, NodeId> nodeMap) throws UaException {
        List<NodeId> childNodeIds = new ArrayList<>();
    
        client.getAddressSpace().browse(nodeId).forEach(ref -> {
            NodeId childId = ref.getNodeId().toNodeId(client.getNamespaceTable()).orElse(null);
            if (childId != null) {
                String browseName = ref.getBrowseName().getName();
    
                nodeMap.put(browseName, childId);
                childNodeIds.add(childId);
    
                log.debug("노드 추가: {} - {}", browseName, childId);
            }
        });
    
        return childNodeIds;
    }
    /**
     * 특정 그룹의 모든 변수값 읽기
     */
    public Map<String, Object> readGroupValues(String groupName) {
        Map<String, Object> result = new HashMap<>();

        if (!connected) {
            log.warn("OPC UA 서버에 연결되어 있지 않습니다");
            result.put("error", "서버에 연결되어 있지 않습니다");
            return result;
        }

        Map<String, NodeId> nodes = groupedNodes.get(groupName);
        if (nodes == null) {
            log.warn("존재하지 않는 그룹: {}", groupName);
            result.put("error", "존재하지 않는 그룹");
            return result;
        }

        for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
            String varName = entry.getKey();
            NodeId nodeId = entry.getValue();

            try {
                CompletableFuture<DataValue> future = client.readValue(0, TimestampsToReturn.Both, nodeId);
                DataValue value = future.get();

                // 값이 null이 아닌 경우에만 결과에 추가
                if (value.getValue().getValue() != null) {
                    result.put(varName, value.getValue().getValue());
                } else {
                    result.put(varName, null);
                }
            } catch (Exception e) {
                log.error("변수 {} 값 읽기 실패: {}", varName, e.getMessage());
                result.put(varName, "읽기 오류");
            }
        }

        return result;
    }

    /**
     * 모든 그룹의 변수값 읽기
     */
    public Map<String, Map<String, Object>> readAllValues() {
        Map<String, Map<String, Object>> result = new HashMap<>();

        for (String groupName : groupedNodes.keySet()) {
            result.put(groupName, readGroupValues(groupName));
        }

        return result;
    }

    /**
     * 연결 상태 확인
     */
    public boolean isConnected() {
        return connected;
    }
}