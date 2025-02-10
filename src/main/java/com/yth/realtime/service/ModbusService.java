package com.yth.realtime.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ghgande.j2mod.modbus.ModbusException;
import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.Register;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class ModbusService {
    private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
    private ModbusTCPMaster modbusMaster1;  // 205용
    private ModbusTCPMaster modbusMaster2;  // 206용
    private final InfluxDBService influxDBService;
    // private final Random random = new Random();  // 랜덤 데이터 생성용

    @Value("${modbus.host1:192.168.10.205}")
    private String host1;
    
    @Value("${modbus.host2:192.168.10.206}")
    private String host2;
    
    @Value("${modbus.port:502}")
    private int port;
    
    @Value("${modbus.timeout:3000}")
    private int timeout;

    @Value("${modbus.slave-id:1}")
    private int slaveId;

    public ModbusService(InfluxDBService influxDBService) {
        this.influxDBService = influxDBService;
    }

    @PostConstruct
    public void init() {
        initModbus1();
        initModbus2();
    }

    private void initModbus1() {
        try {
            modbusMaster1 = new ModbusTCPMaster(host1, port);
            modbusMaster1.setTimeout(timeout);
            modbusMaster1.connect();
            log.info("Modbus1 TCP 연결 성공 - host: {}, port: {}", host1, port);
        } catch (Exception e) {
            log.error("Modbus1 TCP 연결 실패: {}", e.getMessage());
        }
    }

    private void initModbus2() {
        try {
            modbusMaster2 = new ModbusTCPMaster(host2, port);
            modbusMaster2.setTimeout(timeout);
            modbusMaster2.connect();
            log.info("Modbus2 TCP 연결 성공 - host: {}, port: {}", host2, port);
        } catch (Exception e) {
            log.error("Modbus2 TCP 연결 실패: {}", e.getMessage());
        }
    }

    public int[] readModbusData1() {
        return readModbusData(modbusMaster1, "Modbus1", host1);
    }

    public int[] readModbusData2() {
        if (!modbusMaster2.isConnected()) {
            log.error("Modbus2 연결이 되어있지 않습니다. (206)");
            reconnect2();
            return new int[]{0, 0};
        }

        try {
            int startAddress = 12;
            int length = 2;
            Register[] registers = modbusMaster2.readMultipleRegisters(slaveId, startAddress, length);
            
            if (registers == null) {
                log.error("206 레지스터 읽기 실패: null 반환");
                return new int[]{0, 0};
            }

            int[] data = new int[length];
            for (int i = 0; i < length; i++) {
                data[i] = registers[i].getValue();
            }
            log.info("206 데이터 읽기 성공: temperature={}, humidity={}", data[0], data[1]);
            
            // InfluxDB에 데이터 저장
            influxDBService.saveSensorData(data[0] / 10.0, data[1] / 10.0, host2);
            
            return data;
        } catch (ModbusException e) {
            log.error("206 Modbus 데이터 읽기 실패: {}", e.getMessage());
            if (e.getMessage().contains("Connection reset")) {
                reconnect2();
            }
            return new int[]{0, 0};
        }
    }

    private int[] readModbusData(ModbusTCPMaster master, String name, String host) {
        if (!master.isConnected()) {
            log.error("{} 연결이 되어있지 않습니다.", name);
            reconnect(master, name, host);
            return new int[]{0, 0};
        }

        try {
            int startAddress = 10;
            int length = 2;
            Register[] registers = master.readMultipleRegisters(slaveId, startAddress, length);
            
            if (registers == null) {
                log.error("{} 레지스터 읽기 실패: null 반환", name);
                return new int[]{0, 0};
            }

            int[] data = new int[length];
            for (int i = 0; i < length; i++) {
                data[i] = registers[i].getValue();
            }
            
            double temperature = data[0] / 10.0;
            double humidity = data[1] / 10.0;
            
            // 각 장치별로 구분하여 저장
            influxDBService.saveSensorData(temperature, humidity, host);
            
            log.info("{} 데이터 읽기 성공: temperature={}, humidity={}", name, temperature, humidity);
            return data;
        } catch (ModbusException e) {
            log.error("{} 데이터 읽기 실패: {}", name, e.getMessage());
            if (e.getMessage().contains("Connection reset")) {
                reconnect(master, name, host);
            }
            return new int[]{0, 0};
        }
    }

    // 더미 데이터 생성 메서드
    // private int[] generateDummyData() {
    //     try {
    //         // 온도: 20.0 ~ 30.0도 사이의 랜덤값 (소수점 한자리)
    //         int temperature = (int)((random.nextDouble() * 100 + 200)); // 200~300 -> 20.0~30.0
            
    //         // 습도: 40.0 ~ 60.0% 사이의 랜덤값 (소수점 한자리)
    //         int humidity = (int)((random.nextDouble() * 200 + 400)); // 400~600 -> 40.0~60.0
            
    //         int[] data = new int[]{temperature, humidity};
            
    //         // InfluxDB에 데이터 저장
    //         double realTemp = temperature / 10.0;
    //         double realHumidity = humidity / 10.0;
    //         influxDBService.saveSensorData(realTemp, realHumidity);
            
    //         log.info("더미 데이터 생성 - 온도: {}°C, 습도: {}%", realTemp, realHumidity);
            
    //         return data;
    //     } catch (Exception e) {
    //         log.error("더미 데이터 생성 실패: {}", e.getMessage());
    //         return new int[]{0, 0};
    //     }
    // }

    // 실제/더미 데이터 모드 전환을 위한 메서드 (필요시 사용)
    // public void toggleDummyMode() {
    //     // 추후 실제/더미 모드 전환 기능 구현 가능
    // }

    private void reconnect(ModbusTCPMaster master, String name, String host) {
        try {
            if (master.isConnected()) {
                master.disconnect();
            }
            master.connect();
            log.info("{} TCP 재연결 성공", name);
        } catch (Exception e) {
            log.error("{} TCP 재연결 실패: {}", name, e.getMessage());
        }
    }

    private void reconnect2() {
        try {
            if (modbusMaster2.isConnected()) {
                modbusMaster2.disconnect();
            }
            modbusMaster2.connect();
            log.info("206 Modbus TCP 재연결 성공");
        } catch (Exception e) {
            log.error("206 Modbus TCP 재연결 실패: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void disconnect() {
        if (modbusMaster1 != null && modbusMaster1.isConnected()) {
            modbusMaster1.disconnect();
            log.info("Modbus1 TCP 연결 해제");
        }
        if (modbusMaster2 != null && modbusMaster2.isConnected()) {
            modbusMaster2.disconnect();
            log.info("Modbus2 TCP 연결 해제");
        }
    }
} 






// package com.yth.realtime.service;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;

// import com.ghgande.j2mod.modbus.ModbusException;
// import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
// import com.ghgande.j2mod.modbus.procimg.Register;

// import jakarta.annotation.PostConstruct;
// import jakarta.annotation.PreDestroy;

// @Service
// public class ModbusService {
//     private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
//     private ModbusTCPMaster modbusMaster;

//     @Value("${modbus.host:192.168.10.205}")
//     private String host;
    
//     @Value("${modbus.port:502}")
//     private int port;
    
//     @Value("${modbus.timeout:3000}")
//     private int timeout;

//     @Value("${modbus.slave-id:1}")
//     private int slaveId;

//     @PostConstruct
//     public void init() {
//         try {
//             modbusMaster = new ModbusTCPMaster(host, port);
//             modbusMaster.setTimeout(timeout);
//             modbusMaster.connect();
//             log.info("Modbus TCP 연결 성공 - host: {}, port: {}", host, port);
//         } catch (Exception e) {
//             log.error("Modbus TCP 연결 실패: {}", e.getMessage());
//         }
//     }

//     public int[] readModbusData() {
//         if (!modbusMaster.isConnected()) {
//             log.error("Modbus 연결이 되어있지 않습니다.");
//             return new int[]{0, 0};
//         }

//         try {
//             int startAddress = 10;
//             int length = 2;
//             // 슬레이브 ID를 1로 지정하고 읽기
//             Register[] registers = modbusMaster.readMultipleRegisters(slaveId, startAddress, length);
            
//             if (registers == null) {
//                 log.error("레지스터 읽기 실패: null 반환");
//                 return new int[]{0, 0};
//             }

//             int[] data = new int[length];
//             for (int i = 0; i < length; i++) {
//                 data[i] = registers[i].getValue();
//             }
//             log.info("데이터 읽기 성공: temperature={}, humidity={}", data[0], data[1]);
//             return data;
//         } catch (ModbusException e) {
//             log.error("Modbus 데이터 읽기 실패: {}", e.getMessage());
//             if (e.getMessage().contains("Connection reset")) {
//                 reconnect();
//             }
//             return new int[]{0, 0};
//         }
//     }

//     private void reconnect() {
//         try {
//             if (modbusMaster.isConnected()) {
//                 modbusMaster.disconnect();
//             }
//             modbusMaster.connect();
//             log.info("Modbus TCP 재연결 성공");
//         } catch (Exception e) {
//             log.error("Modbus TCP 재연결 실패: {}", e.getMessage());
//         }
//     }

//     @PreDestroy
//     public void disconnect() {
//         if (modbusMaster != null && modbusMaster.isConnected()) {
//             modbusMaster.disconnect();
//             log.info("Modbus TCP 연결 해제");
//         }
//     }
// } 