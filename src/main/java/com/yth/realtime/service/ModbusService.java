package com.yth.realtime.service;

import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import net.wimpi.modbus.ModbusException;
import net.wimpi.modbus.io.ModbusTCPTransaction;
import net.wimpi.modbus.msg.ReadInputRegistersRequest;
import net.wimpi.modbus.msg.ReadInputRegistersResponse;
import net.wimpi.modbus.net.TCPMasterConnection;

@Service
public class ModbusService {
    private TCPMasterConnection connection;
    private static final String IP_ADDRESS = "10.11.17.103";
    private static final int PORT = 502;
    private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
    
    // Modbus 레지스터 설정
    private static final int REGISTER_START_ADDRESS = 10;  // 시작 주소를 0으로 변경
    private static final int REGISTER_COUNT = 2;         // 읽을 레지스터 개수

    public ModbusService() {
        initConnection();
    }

    private void initConnection() {
        try {
            if (connection != null && connection.isConnected()) {
                connection.close();
            }
            InetAddress address = InetAddress.getByName(IP_ADDRESS);
            connection = new TCPMasterConnection(address);
            connection.setPort(PORT);
            connection.connect();
            log.info("Modbus 연결 성공: {}:{}", IP_ADDRESS, PORT);
        } catch (Exception e) {
            log.error("Modbus 연결 실패: {}", e.getMessage());
        }
    }

    public int[] readModbusData() {
        if (connection == null || !connection.isConnected()) {
            log.info("Modbus 재연결 시도");
            initConnection();
        }
        
        try {
            // 레지스터 읽기 요청 생성
            ReadInputRegistersRequest request = 
                new ReadInputRegistersRequest(REGISTER_START_ADDRESS, REGISTER_COUNT);
            
            // 트랜잭션 설정 및 실행
            ModbusTCPTransaction transaction = new ModbusTCPTransaction(connection);
            transaction.setRequest(request);
            
            // 요청 실행 전 로그
            log.debug("Modbus 데이터 요청 - 시작주소: {}, 개수: {}", 
                REGISTER_START_ADDRESS, REGISTER_COUNT);
            
            transaction.execute();
            
            // 응답 처리
            ReadInputRegistersResponse response = 
                (ReadInputRegistersResponse) transaction.getResponse();
            
            int temp = response.getRegister(0).getValue();
            int humidity = response.getRegister(1).getValue();
            
            log.info("Modbus 데이터 읽기 성공 - 온도: {}, 습도: {}", temp, humidity);
            return new int[] {temp, humidity};
            
        } catch (ModbusException e) {
            log.error("Modbus 데이터 읽기 실패: {}", e.getMessage());
            return new int[] {0, 0};
        } catch (Exception e) {
            log.error("예상치 못한 오류 발생: {}", e.getMessage());
            return new int[] {0, 0};
        }
    }
} 