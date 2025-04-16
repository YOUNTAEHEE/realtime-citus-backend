//태그호출
// package com.yth.realtime.service;

// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.LinkedHashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.ExecutionException;
// import java.util.concurrent.atomic.AtomicLong;
// import java.util.stream.Collectors;

// import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
// import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
// import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
// import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
// import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
// import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
// import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.stereotype.Component;

// @Component
// public class OpcuaClient {
//     private static final Logger log = LoggerFactory.getLogger(OpcuaClient.class);

//     private OpcUaClient client;
//     private boolean connected = false;
//     private final Map<String, Map<String, NodeId>> groupedNodes = new HashMap<>();
//     private static final String PLC_VARIABLES_GROUP = "PLC_Variables";
//     private final AtomicLong clientHandleCounter = new AtomicLong(1);
//     private static final String SERVER_URL = "opc.tcp://192.168.10.35:4840";

//     private static final Map<String, String> TARGET_TAGS_CONFIG = new LinkedHashMap<>() {
//         {
//             String basePath = "ns=4;s=|var|CODESYS Control Win V3 x64.Application.His10ms.";
//             put("Filtered_Grid_Freq", basePath + "Filtered_Grid_Freq");
//             put("Grid_Freq", basePath + "Grid_Freq");
//             put("Grid_Vol_A", basePath + "Grid_Vol_A");
//             put("Grid_Vol_B", basePath + "Grid_Vol_B");
//             put("Grid_Vol_C", basePath + "Grid_Vol_C");
//             put("PCS1_PF_OUT", basePath + "PCS1_PF_OUT");
//             put("PCS1_SOC", basePath + "PCS1_SOC");
//             put("PCS1_STATUS", basePath + "PCS1_STATUS");
//             put("PCS1_TPWR_P_FB", basePath + "PCS1_TPWR_P_FB");
//             put("PCS1_TPWR_P_REAL", basePath + "PCS1_TPWR_P_REAL");
//             put("PCS1_TPWR_P_REF", basePath + "PCS1_TPWR_P_REF");
//             put("PCS1_TPWR_Q_FB", basePath + "PCS1_TPWR_Q_FB");
//             put("PCS1_TPWR_Q_REAL", basePath + "PCS1_TPWR_Q_REAL");
//             put("PCS2_PF_OUT", basePath + "PCS2_PF_OUT");
//             put("PCS2_SOC", basePath + "PCS2_SOC");
//             put("PCS2_STATUS", basePath + "PCS2_STATUS");
//             put("PCS2_TPWR_P_FB", basePath + "PCS2_TPWR_P_FB");
//             put("PCS2_TPWR_P_REAL", basePath + "PCS2_TPWR_P_REAL");
//             put("PCS2_TPWR_P_REF", basePath + "PCS2_TPWR_P_REF");
//             put("PCS2_TPWR_Q_FB", basePath + "PCS2_TPWR_Q_FB");
//             put("PCS2_TPWR_Q_REAL", basePath + "PCS2_TPWR_Q_REAL");
//             put("PCS3_PF_OUT", basePath + "PCS3_PF_OUT");
//             put("PCS3_SOC", basePath + "PCS3_SOC");
//             put("PCS3_STATUS", basePath + "PCS3_STATUS");
//             put("PCS3_TPWR_P_FB", basePath + "PCS3_TPWR_P_FB");
//             put("PCS3_TPWR_P_REAL", basePath + "PCS3_TPWR_P_REAL");
//             put("PCS3_TPWR_P_REF", basePath + "PCS3_TPWR_P_REF");
//             put("PCS3_TPWR_Q_FB", basePath + "PCS3_TPWR_Q_FB");
//             put("PCS3_TPWR_Q_REAL", basePath + "PCS3_TPWR_Q_REAL");
//             put("PCS4_PF_OUT", basePath + "PCS4_PF_OUT");
//             put("PCS4_SOC", basePath + "PCS4_SOC");
//             put("PCS4_STATUS", basePath + "PCS4_STATUS");
//             put("PCS4_TPWR_P_FB", basePath + "PCS4_TPWR_P_FB");
//             put("PCS4_TPWR_P_REAL", basePath + "PCS4_TPWR_P_REAL");
//             put("PCS4_TPWR_P_REF", basePath + "PCS4_TPWR_P_REF");
//             put("PCS4_TPWR_Q_FB", basePath + "PCS4_TPWR_Q_FB");
//             put("PCS4_TPWR_Q_REAL", basePath + "PCS4_TPWR_Q_REAL");
//             put("PMS1_Mode_State", basePath + "PMS1_Mode_State");
//             put("PMS2_Mode_State", basePath + "PMS2_Mode_State");
//             put("PMS3_Mode_State", basePath + "PMS3_Mode_State");
//             put("PMS4_Mode_State", basePath + "PMS4_Mode_State");
//             put("Rev0", basePath + "Rev0");
//             put("Rev_P1_1", basePath + "Rev_P1_1");
//             put("Rev_P1_2", basePath + "Rev_P1_2");
//             put("Rev_P2_1", basePath + "Rev_P2_1");
//             put("Rev_P2_2", basePath + "Rev_P2_2");
//             put("Rev_P3_1", basePath + "Rev_P3_1");
//             put("Rev_P3_2", basePath + "Rev_P3_2");
//             put("Rev_P4_1", basePath + "Rev_P4_1");
//             put("Rev_P4_2", basePath + "Rev_P4_2");
//             put("T_Simul_P_REAL", basePath + "T_Simul_P_REAL");
//             put("Total_TPWR_P_REAL", basePath + "Total_TPWR_P_REAL");
//             put("Total_TPWR_P_REF", basePath + "Total_TPWR_P_REF");
//             put("Total_TPWR_Q_REAL", basePath + "Total_TPWR_Q_REAL");
//             put("Total_TPWR_Q_REF", basePath + "Total_TPWR_Q_REF");
//         }
//     };

//     private final Map<String, NodeId> targetNodes = TARGET_TAGS_CONFIG.entrySet().stream()
//             .collect(Collectors.toMap(
//                     Map.Entry::getKey,
//                     entry -> {
//                         try {
//                             return NodeId.parse(entry.getValue());
//                         } catch (Exception e) {
//                             log.error("Failed to parse NodeId: {}", entry.getValue(), e);
//                             return null;
//                         }
//                     },
//                     (v1, v2) -> v1,
//                     LinkedHashMap::new));

//     /**
//      * OPC UA 서버에 연결
//      */
//     public boolean connect() {
//         if (connected) {
//             log.info("이미 OPC UA 서버에 연결되어 있습니다.");
//             return true;
//         }

//         try {
//             List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(SERVER_URL).get();
//             EndpointDescription endpoint = endpoints.stream()
//                     .findFirst()
//                     .orElseThrow(() -> new RuntimeException("No endpoints found"));

//             OpcUaClientConfigBuilder configBuilder = new OpcUaClientConfigBuilder();
//             configBuilder.setEndpoint(endpoint);

//             client = OpcUaClient.create(configBuilder.build());
//             client.connect().get();
//             connected = true;

//             log.info("OPC UA 서버에 연결되었습니다: {}", SERVER_URL);

//             groupedNodes.put(PLC_VARIABLES_GROUP, new LinkedHashMap<>(targetNodes));
//             log.info("지정된 {}개의 태그를 {} 그룹에 로드했습니다.", targetNodes.size(), PLC_VARIABLES_GROUP);

//             return true;
//         } catch (Exception e) {
//             log.error("OPC UA 서버 연결 실패: {}", e.getMessage(), e);
//             connected = false;
//             return false;
//         }
//     }

//     /**
//      * OPC UA 서버와의 연결 해제
//      */
//     public void disconnect() {
//         if (client != null && connected) {
//             try {
//                 client.disconnect().get();
//                 connected = false;
//                 groupedNodes.clear();
//                 log.info("OPC UA 서버 연결이 종료되었습니다");
//             } catch (Exception e) {
//                 log.error("OPC UA 서버 연결 해제 실패: {}", e.getMessage(), e);
//             }
//         }
//     }

//     /**
//      * 특정 그룹의 모든 변수값 읽기
//      */
//     public Map<String, Object> readGroupValues(String groupName) {
//         Map<String, Object> result = new LinkedHashMap<>();

//         if (!connected) {
//             log.warn("OPC UA 서버에 연결되어 있지 않습니다");
//             result.put("error", "서버에 연결되어 있지 않습니다");
//             return result;
//         }

//         if (!PLC_VARIABLES_GROUP.equals(groupName)) {
//             log.warn("정의되지 않은 그룹 요청: {}", groupName);
//             result.put("error", "정의되지 않은 그룹");
//             return result;
//         }

//         Map<String, NodeId> nodes = groupedNodes.get(groupName);
//         if (nodes == null || nodes.isEmpty()) {
//             log.warn("그룹 '{}'에 로드된 노드가 없습니다.", groupName);
//             result.put("error", "그룹에 노드가 없습니다");
//             return result;
//         }

//         try {
//             List<NodeId> nodeIdList = new ArrayList<>(nodes.values());
//             List<NodeId> validNodeIdList = nodeIdList.stream()
//                     .filter(java.util.Objects::nonNull)
//                     .collect(Collectors.toList());

//             if (validNodeIdList.isEmpty()) {
//                 log.warn("그룹 '{}'에 유효한 NodeId가 없습니다.", groupName);
//                 result.put("error", "유효한 NodeId 없음");
//                 return result;
//             }

//             log.debug("Reading {} values for group '{}'", validNodeIdList.size(), groupName);
//             List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, validNodeIdList).get();

//             int validIndex = 0;
//             for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
//                 String varName = entry.getKey();
//                 NodeId nodeId = entry.getValue();

//                 if (nodeId == null) {
//                     result.put(varName, "NodeId 파싱 오류");
//                     continue;
//                 }

//                 if (validIndex < values.size()) {
//                     DataValue value = values.get(validIndex++);
//                     if (value != null && value.getValue() != null && value.getStatusCode().isGood()) {
//                         result.put(varName, value.getValue().getValue());
//                     } else {
//                         result.put(varName, null);
//                         log.warn("Failed to read or bad quality for tag '{}' (NodeId: {}). Status: {}",
//                                 varName, nodeId, value != null ? value.getStatusCode() : "null DataValue");
//                     }
//                 } else {
//                     log.error("Mismatch between node list and read results for group '{}'", groupName);
//                     result.put(varName, "결과 불일치 오류");
//                 }
//             }
//         } catch (InterruptedException | ExecutionException e) {
//             log.error("그룹 {} 값 일괄 읽기 실패: {}", groupName, e.getMessage(), e);
//             if (e instanceof InterruptedException) {
//                 Thread.currentThread().interrupt();
//             }
//             nodes.keySet().forEach(varName -> result.putIfAbsent(varName, "일괄 읽기 실패"));
//             result.put("error", "일괄 읽기 실패: " + e.getMessage());
//         } catch (Exception e) {
//             log.error("그룹 {} 값 일괄 읽기 중 예상치 못한 오류 발생: {}", groupName, e.getMessage(), e);
//             nodes.keySet().forEach(varName -> result.putIfAbsent(varName, "알 수 없는 오류"));
//             result.put("error", "알 수 없는 오류: " + e.getMessage());
//         }

//         return result;
//     }

//     /**
//      * 모든 그룹의 변수값 읽기
//      */
//     public Map<String, Map<String, Object>> readAllValues() {
//         Map<String, Map<String, Object>> result = new HashMap<>();

//         if (!groupedNodes.isEmpty()) {
//             result.put(PLC_VARIABLES_GROUP, readGroupValues(PLC_VARIABLES_GROUP));
//         } else {
//             log.warn("읽을 그룹이 없습니다. 서버에 연결되었는지 확인하세요.");
//             Map<String, Object> errorMap = new HashMap<>();
//             errorMap.put("error", "읽을 그룹 없음");
//             result.put(PLC_VARIABLES_GROUP, errorMap);
//         }

//         return result;
//     }

//     /**
//      * 연결 상태 확인
//      */
//     public boolean isConnected() {
//         return connected;
//     }
// }



//오브젝트호출
package com.yth.realtime.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
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
    private final AtomicLong clientHandleCounter = new AtomicLong(1);
    // OPC UA 서버 주소 및 노드 ID 설정
    private static final String SERVER_URL = "opc.tcp://192.168.10.12:4840";
    // private static final String SERVER_URL = "opc.tcp://192.168.0.30:4840";
    // private static final String SERVER_URL = "opc.tcp://CIT-JIN:4840";
    // private static final String SERVER_URL = "opc.tcp://192.168.10.35:4840";
    // private static final String SERVER_URL = "opc.tcp://192.168.0.77:4840";

    private static final Map<String, String> OBJECT_NODES = Map.of(
            // "OPC_UA", "ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG",
            // "PLC_PRG", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.PLC_PRG",
            // "GVL", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.GVL",
        //    "HIS", "ns=4;s=|var|CODESYS Control Win V3 x64.Application.His10ms"
           //,
            "HISto", "ns=4;s=|var|CODESYS Control Win V3 x64.FRC1.Historian.fHistorian"
            );

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
//구독방식추가
    // public void startSubscription(ValueUpdateHandler handler) {
    //     if (!connected) {
    //         log.warn("OPC UA 서버에 연결되어 있지 않으므로 구독을 시작할 수 없습니다.");
    //         return;
    //     }
    
    //     try {
    //         UaSubscription subscription = client.getSubscriptionManager()
    //                 .createSubscription(1000.0).get(); // 1초 샘플링 주기
    
    //         for (Map.Entry<String, Map<String, NodeId>> groupEntry : groupedNodes.entrySet()) {
    //             String groupName = groupEntry.getKey();
    //             Map<String, NodeId> nodeMap = groupEntry.getValue();
    
    //             for (Map.Entry<String, NodeId> nodeEntry : nodeMap.entrySet()) {
    //                 String varName = nodeEntry.getKey();
    //                 NodeId nodeId = nodeEntry.getValue();
    
    //                 ReadValueId readValueId = new ReadValueId(
    //                         nodeId, AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
    
    //                 // MonitoringParameters parameters = new MonitoringParameters(
    //                 //         Unsigned.uint(1), // client handle
    //                 //         1000.0, // 샘플링 주기(ms)
    //                 //         null,
    //                 //         Unsigned.uint(10),
    //                 //         true);
    
    //                 long clientHandle = clientHandleCounter.getAndIncrement();

    //                 MonitoringParameters parameters = new MonitoringParameters(
    //                         Unsigned.uint(clientHandle),
    //                         1000.0,
    //                         null, // Deadband 없음 → 작은 변화도 무시하지 않음
    //                         Unsigned.uint(10),
    //                         true);

    //                 MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
    //                         readValueId, MonitoringMode.Reporting, parameters);
    
    //                 UaSubscription.ItemCreationCallback onItemCreated = (item, id) -> {
    //                     item.setValueConsumer((itemVal, val) -> {
    //                         Object value = val.getValue().getValue();
    //                         handler.handleValueUpdate(groupName, varName, value);
    //                     });
    //                 };
    
    //                 subscription.createMonitoredItems(
    //                         TimestampsToReturn.Both,
    //                         List.of(request),
    //                         onItemCreated
    //                 ).get();
    //             }
    //         }
    
    //         log.info("Subscription 구독 방식 초기화 완료");
    
    //     } catch (Exception e) {
    //         log.error("Subscription 등록 중 오류 발생: {}", e.getMessage(), e);
    //     }
    // }
    
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
    // public Map<String, Object> readGroupValues(String groupName) {
    //     Map<String, Object> result = new HashMap<>();

    //     if (!connected) {
    //         log.warn("OPC UA 서버에 연결되어 있지 않습니다");
    //         result.put("error", "서버에 연결되어 있지 않습니다");
    //         return result;
    //     }

    //     Map<String, NodeId> nodes = groupedNodes.get(groupName);
    //     if (nodes == null) {
    //         log.warn("존재하지 않는 그룹: {}", groupName);
    //         result.put("error", "존재하지 않는 그룹");
    //         return result;
    //     }

    //     for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
    //         String varName = entry.getKey();
    //         NodeId nodeId = entry.getValue();

    //         try {
    //             CompletableFuture<DataValue> future = client.readValue(0, TimestampsToReturn.Both, nodeId);
    //             DataValue value = future.get();

    //             // 값이 null이 아닌 경우에만 결과에 추가
    //             if (value.getValue().getValue() != null) {
    //                 result.put(varName, value.getValue().getValue());
    //             } else {
    //                 result.put(varName, null);
    //             }
    //         } catch (Exception e) {
    //             log.error("변수 {} 값 읽기 실패: {}", varName, e.getMessage());
    //             result.put(varName, "읽기 오류");
    //         }
    //     }

    //     return result;
    // }
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
    
        try {
            List<NodeId> nodeIdList = new ArrayList<>(nodes.values());
            List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, nodeIdList).get();
    
            int index = 0;
            for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
                String varName = entry.getKey();
                DataValue value = values.get(index++);
    
                if (value != null && value.getValue() != null) {
                    result.put(varName, value.getValue().getValue());
                } else {
                    result.put(varName, null);
                }
            }
        } catch (Exception e) {
            log.error("그룹 {} 값 일괄 읽기 실패: {}", groupName, e.getMessage(), e);
            result.put("error", "일괄 읽기 실패");
        }
    
        return result;
    }
    

    /**
     * 모든 그룹의 변수값 읽기
     */
    // public Map<String, Map<String, Object>> readAllValues() {
    //     Map<String, Map<String, Object>> result = new HashMap<>();

    //     for (String groupName : groupedNodes.keySet()) {
    //         result.put(groupName, readGroupValues(groupName));
    //     }

    //     return result;
    // }

      /**
     * 모든 그룹의 변수값 읽기 (그룹 단위 병렬 처리)
     */
    public Map<String, Map<String, Object>> readAllValues() {
        Map<String, Map<String, Object>> result = new HashMap<>();

        if (!connected) {
            log.warn("OPC UA 서버에 연결되어 있지 않습니다. 빈 결과를 반환합니다.");
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("error", "서버에 연결되어 있지 않습니다");
            // 어떤 그룹 이름으로 넣을지 애매하므로, 특정 그룹(예: HIS) 또는 별도 키 사용 고려
            // 여기서는 일단 빈 Map 반환 또는 특정 에러 그룹 추가 가능
            // result.put("ERROR_GROUP", errorMap); // 예시
            return result; // 또는 에러 정보를 포함한 result 반환
        }

        if (groupedNodes.isEmpty()) {
            log.warn("초기화된 그룹 노드가 없습니다.");
            return result;
        }

        // 그룹 단위로 병렬 수집 작업 생성
        List<CompletableFuture<Map.Entry<String, Map<String, Object>>>> tasks = groupedNodes.entrySet().stream()
                .map(entry -> CompletableFuture.supplyAsync(() -> {
                    String group = entry.getKey();
                    // readGroupValues는 내부적으로 예외 처리 및 로깅을 포함함
                    Map<String, Object> values = readGroupValues(group);
                    // Map.entry를 사용하여 그룹 이름과 결과 값을 쌍으로 반환
                    return Map.entry(group, values);
                }/*, collectorPool */)) // 특정 ExecutorService 사용 시 주석 해제 및 전달 필요
                .collect(Collectors.toList()); // Java 16+ 에서는 .toList() 사용 가능

        // 모든 비동기 작업이 완료될 때까지 기다리고 결과 취합
        try {
            // CompletableFuture.allOf를 사용하여 모든 작업이 완료되기를 기다림
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]));

            // 예시: 전체 작업에 타임아웃 설정 (10초)
            allFutures.get(10, TimeUnit.SECONDS);

            // 모든 작업이 성공적으로 완료된 후 결과 취합 (예외가 발생하지 않은 경우)
            for (CompletableFuture<Map.Entry<String, Map<String, Object>>> task : tasks) {
                // 이제 get()은 예외 없이 결과를 반환 (이미 완료되었으므로)
                // 만약 개별 작업에서 예외가 발생했다면 allFutures.get()에서 예외가 터졌을 것임
                Map.Entry<String, Map<String, Object>> groupResult = task.join(); // 예외 없이 결과 가져오기 (이미 완료됨)
                result.put(groupResult.getKey(), groupResult.getValue());
            }

        } catch (InterruptedException e) {
            log.error("모든 그룹 값 읽기 대기 중 인터럽트 발생", e);
            Thread.currentThread().interrupt();
            result.put("ERROR", Map.of("error", "읽기 작업 인터럽트됨"));
        } catch (ExecutionException e) {
            log.error("그룹 값 읽기 작업 중 하나 이상 실패", e.getCause());
            result.put("ERROR", Map.of("error", "읽기 작업 실패: " + e.getCause().getMessage()));
            // 실패 시 부분 결과라도 반환할지 결정 필요
            // 예: 성공한 결과만이라도 넣어주기
            for (CompletableFuture<Map.Entry<String, Map<String, Object>>> task : tasks) {
                if (task.isDone() && !task.isCompletedExceptionally()) {
                    Map.Entry<String, Map<String, Object>> groupResult = task.join();
                    result.putIfAbsent(groupResult.getKey(), groupResult.getValue());
                }
            }
        } catch (Exception e) { // TimeoutException 등 기타 예외
            log.error("모든 그룹 값 읽기 중 예상치 못한 오류 발생", e);
            result.put("ERROR", Map.of("error", "읽기 작업 중 오류: " + e.getMessage()));
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