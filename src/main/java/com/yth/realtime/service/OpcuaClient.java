
package com.yth.realtime.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
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
    private static final String PLC_VARIABLES_GROUP = "PLC_Variables";
    private final AtomicLong clientHandleCounter = new AtomicLong(1);
    private static final String SERVER_URL = "opc.tcp://192.168.10.35:4840";

    private static final Map<String, String> TARGET_TAGS_CONFIG = new LinkedHashMap<>() {
        {
            String basePath = "ns=4;s=|var|CODESYS Control Win V3 x64.Application.His10ms.";
            put("Filtered_Grid_Freq", basePath + "Filtered_Grid_Freq");
            put("Grid_Freq", basePath + "Grid_Freq");
            put("Grid_Vol_A", basePath + "Grid_Vol_A");
            put("Grid_Vol_B", basePath + "Grid_Vol_B");
            put("Grid_Vol_C", basePath + "Grid_Vol_C");
            put("PCS1_PF_OUT", basePath + "PCS1_PF_OUT");
            put("PCS1_SOC", basePath + "PCS1_SOC");
            put("PCS1_STATUS", basePath + "PCS1_STATUS");
            put("PCS1_TPWR_P_FB", basePath + "PCS1_TPWR_P_FB");
            put("PCS1_TPWR_P_REAL", basePath + "PCS1_TPWR_P_REAL");
            put("PCS1_TPWR_P_REF", basePath + "PCS1_TPWR_P_REF");
            put("PCS1_TPWR_Q_FB", basePath + "PCS1_TPWR_Q_FB");
            put("PCS1_TPWR_Q_REAL", basePath + "PCS1_TPWR_Q_REAL");
            put("PCS2_PF_OUT", basePath + "PCS2_PF_OUT");
            put("PCS2_SOC", basePath + "PCS2_SOC");
            put("PCS2_STATUS", basePath + "PCS2_STATUS");
            put("PCS2_TPWR_P_FB", basePath + "PCS2_TPWR_P_FB");
            put("PCS2_TPWR_P_REAL", basePath + "PCS2_TPWR_P_REAL");
            put("PCS2_TPWR_P_REF", basePath + "PCS2_TPWR_P_REF");
            put("PCS2_TPWR_Q_FB", basePath + "PCS2_TPWR_Q_FB");
            put("PCS2_TPWR_Q_REAL", basePath + "PCS2_TPWR_Q_REAL");
            put("PCS3_PF_OUT", basePath + "PCS3_PF_OUT");
            put("PCS3_SOC", basePath + "PCS3_SOC");
            put("PCS3_STATUS", basePath + "PCS3_STATUS");
            put("PCS3_TPWR_P_FB", basePath + "PCS3_TPWR_P_FB");
            put("PCS3_TPWR_P_REAL", basePath + "PCS3_TPWR_P_REAL");
            put("PCS3_TPWR_P_REF", basePath + "PCS3_TPWR_P_REF");
            put("PCS3_TPWR_Q_FB", basePath + "PCS3_TPWR_Q_FB");
            put("PCS3_TPWR_Q_REAL", basePath + "PCS3_TPWR_Q_REAL");
            put("PCS4_PF_OUT", basePath + "PCS4_PF_OUT");
            put("PCS4_SOC", basePath + "PCS4_SOC");
            put("PCS4_STATUS", basePath + "PCS4_STATUS");
            put("PCS4_TPWR_P_FB", basePath + "PCS4_TPWR_P_FB");
            put("PCS4_TPWR_P_REAL", basePath + "PCS4_TPWR_P_REAL");
            put("PCS4_TPWR_P_REF", basePath + "PCS4_TPWR_P_REF");
            put("PCS4_TPWR_Q_FB", basePath + "PCS4_TPWR_Q_FB");
            put("PCS4_TPWR_Q_REAL", basePath + "PCS4_TPWR_Q_REAL");
            put("PMS1_Mode_State", basePath + "PMS1_Mode_State");
            put("PMS2_Mode_State", basePath + "PMS2_Mode_State");
            put("PMS3_Mode_State", basePath + "PMS3_Mode_State");
            put("PMS4_Mode_State", basePath + "PMS4_Mode_State");
            put("Rev0", basePath + "Rev0");
            put("Rev_P1_1", basePath + "Rev_P1_1");
            put("Rev_P1_2", basePath + "Rev_P1_2");
            put("Rev_P2_1", basePath + "Rev_P2_1");
            put("Rev_P2_2", basePath + "Rev_P2_2");
            put("Rev_P3_1", basePath + "Rev_P3_1");
            put("Rev_P3_2", basePath + "Rev_P3_2");
            put("Rev_P4_1", basePath + "Rev_P4_1");
            put("Rev_P4_2", basePath + "Rev_P4_2");
            put("T_Simul_P_REAL", basePath + "T_Simul_P_REAL");
            put("Total_TPWR_P_REAL", basePath + "Total_TPWR_P_REAL");
            put("Total_TPWR_P_REF", basePath + "Total_TPWR_P_REF");
            put("Total_TPWR_Q_REAL", basePath + "Total_TPWR_Q_REAL");
            put("Total_TPWR_Q_REF", basePath + "Total_TPWR_Q_REF");
        }
    };

    private final Map<String, NodeId> targetNodes = TARGET_TAGS_CONFIG.entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        try {
                            return NodeId.parse(entry.getValue());
                        } catch (Exception e) {
                            log.error("Failed to parse NodeId: {}", entry.getValue(), e);
                            return null;
                        }
                    },
                    (v1, v2) -> v1,
                    LinkedHashMap::new));

    /**
     * OPC UA 서버에 연결
     */
    public boolean connect() {
        if (connected) {
            log.info("이미 OPC UA 서버에 연결되어 있습니다.");
            return true;
        }

        try {
            List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(SERVER_URL).get();
            EndpointDescription endpoint = endpoints.stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No endpoints found"));

            OpcUaClientConfigBuilder configBuilder = new OpcUaClientConfigBuilder();
            configBuilder.setEndpoint(endpoint);

            client = OpcUaClient.create(configBuilder.build());
            client.connect().get();
            connected = true;

            log.info("OPC UA 서버에 연결되었습니다: {}", SERVER_URL);

            groupedNodes.put(PLC_VARIABLES_GROUP, new LinkedHashMap<>(targetNodes));
            log.info("지정된 {}개의 태그를 {} 그룹에 로드했습니다.", targetNodes.size(), PLC_VARIABLES_GROUP);

            return true;
        } catch (Exception e) {
            log.error("OPC UA 서버 연결 실패: {}", e.getMessage(), e);
            connected = false;
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
                groupedNodes.clear();
                log.info("OPC UA 서버 연결이 종료되었습니다");
            } catch (Exception e) {
                log.error("OPC UA 서버 연결 해제 실패: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 특정 그룹의 모든 변수값 읽기
     */
    public Map<String, Object> readGroupValues(String groupName) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (!connected) {
            log.warn("OPC UA 서버에 연결되어 있지 않습니다");
            result.put("error", "서버에 연결되어 있지 않습니다");
            return result;
        }

        if (!PLC_VARIABLES_GROUP.equals(groupName)) {
            log.warn("정의되지 않은 그룹 요청: {}", groupName);
            result.put("error", "정의되지 않은 그룹");
            return result;
        }

        Map<String, NodeId> nodes = groupedNodes.get(groupName);
        if (nodes == null || nodes.isEmpty()) {
            log.warn("그룹 '{}'에 로드된 노드가 없습니다.", groupName);
            result.put("error", "그룹에 노드가 없습니다");
            return result;
        }

        try {
            List<NodeId> nodeIdList = new ArrayList<>(nodes.values());
            List<NodeId> validNodeIdList = nodeIdList.stream()
                    .filter(java.util.Objects::nonNull)
                    .collect(Collectors.toList());

            if (validNodeIdList.isEmpty()) {
                log.warn("그룹 '{}'에 유효한 NodeId가 없습니다.", groupName);
                result.put("error", "유효한 NodeId 없음");
                return result;
            }

            log.debug("Reading {} values for group '{}'", validNodeIdList.size(), groupName);
            List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, validNodeIdList).get();

            int validIndex = 0;
            for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
                String varName = entry.getKey();
                NodeId nodeId = entry.getValue();

                if (nodeId == null) {
                    result.put(varName, "NodeId 파싱 오류");
                    continue;
                }

                if (validIndex < values.size()) {
                    DataValue value = values.get(validIndex++);
                    if (value != null && value.getValue() != null && value.getStatusCode().isGood()) {
                        result.put(varName, value.getValue().getValue());
                    } else {
                        result.put(varName, null);
                        log.warn("Failed to read or bad quality for tag '{}' (NodeId: {}). Status: {}",
                                varName, nodeId, value != null ? value.getStatusCode() : "null DataValue");
                    }
                } else {
                    log.error("Mismatch between node list and read results for group '{}'", groupName);
                    result.put(varName, "결과 불일치 오류");
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("그룹 {} 값 일괄 읽기 실패: {}", groupName, e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            nodes.keySet().forEach(varName -> result.putIfAbsent(varName, "일괄 읽기 실패"));
            result.put("error", "일괄 읽기 실패: " + e.getMessage());
        } catch (Exception e) {
            log.error("그룹 {} 값 일괄 읽기 중 예상치 못한 오류 발생: {}", groupName, e.getMessage(), e);
            nodes.keySet().forEach(varName -> result.putIfAbsent(varName, "알 수 없는 오류"));
            result.put("error", "알 수 없는 오류: " + e.getMessage());
        }

        return result;
    }

    /**
     * 모든 그룹의 변수값 읽기
     */
    public Map<String, Map<String, Object>> readAllValues() {
        Map<String, Map<String, Object>> result = new HashMap<>();

        if (!groupedNodes.isEmpty()) {
            result.put(PLC_VARIABLES_GROUP, readGroupValues(PLC_VARIABLES_GROUP));
        } else {
            log.warn("읽을 그룹이 없습니다. 서버에 연결되었는지 확인하세요.");
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("error", "읽을 그룹 없음");
            result.put(PLC_VARIABLES_GROUP, errorMap);
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