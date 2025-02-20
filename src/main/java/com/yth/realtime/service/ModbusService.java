// package com.yth.realtime.service;

// import java.util.Collections;
// import java.util.List;
// import java.util.Map;
// import java.util.Objects;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.CopyOnWriteArrayList;
// import java.util.stream.Collectors;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.stereotype.Service;

// import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
// import com.ghgande.j2mod.modbus.procimg.Register;
// import com.yth.realtime.dto.ModbusDevice;
// import com.yth.realtime.entity.ModbusDeviceDocument;
// import com.yth.realtime.repository.ModbusDeviceRepository;

// import jakarta.annotation.PreDestroy;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class ModbusService {
//     private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
//     private final InfluxDBService influxDBService;
//     private final List<ModbusDevice> registeredDevices = new CopyOnWriteArrayList<>();
//     private final Map<String, ModbusTCPMaster> modbusMasters = new ConcurrentHashMap<>();
//     private final Map<String, Long> lastDataSaveTime = new ConcurrentHashMap<>();
//     private static final long SAVE_INTERVAL = 5000; // 5초
//     private final ModbusDeviceRepository modbusDeviceRepository;

//     public ModbusService(InfluxDBService influxDBService, ModbusDeviceRepository modbusDeviceRepository) {
//         this.influxDBService = influxDBService;
//         this.modbusDeviceRepository = modbusDeviceRepository;
//     }

//     /**
//      * 장치를 등록하는 메서드
//      */
//     public boolean addDevice(ModbusDevice device) {
//         try {
           
//             // 장치 연결 테스트
//             ModbusTCPMaster master = new ModbusTCPMaster(device.getHost(), device.getPort());
//             master.setTimeout(1000);
//             master.setRetries(1);
//             master.connect();  // 연결 시도
            
//             // 테스트 읽기 수행
//             Register[] testRead = master.readMultipleRegisters(
//                 device.getSlaveId(), 
//                 device.getStartAddress(), 
//                 device.getLength()
//             );
            
//             if (testRead != null && testRead.length > 0) {
//                 modbusMasters.put(device.getDeviceId(), master);
//                 registeredDevices.add(device);
//                 if(!modbusDeviceRepository.existsByDeviceId(device.getDeviceId())){
//                     modbusDeviceRepository.save(ModbusDeviceDocument.from(device));
//                 }
//                 log.info("장치 연결 성공: {}", device);
//                 return true;
//             } else {
//                 throw new RuntimeException("장치 응답 없음");
//             }
//         } catch (Exception e) {
//             log.error("장치 연결 실패: {} - {}", device.getDeviceId(), e.getMessage());
//             return false;
//         }
//     }

//     /**
//      * Modbus 데이터를 읽어서 InfluxDB에 저장하고 반환
//      */
//     public int[] readModbusData(ModbusDevice device) throws Exception {
//         ModbusTCPMaster master = getOrCreateConnection(device);
//         try {
//             Register[] registers = master.readMultipleRegisters(
//                 device.getSlaveId(),
//                 device.getStartAddress(),
//                 device.getLength()
//             );

//             int[] data = new int[]{registers[0].getValue(), registers[1].getValue()};
            
//             // 마지막 저장 시간 확인
//             long currentTime = System.currentTimeMillis();
//             Long lastSave = lastDataSaveTime.get(device.getDeviceId());
            
//             // 마지막 저장으로부터 5초가 지났거나 처음 저장하는 경우에만 저장
//             if (lastSave == null || currentTime - lastSave >= SAVE_INTERVAL) {
//                 double temperature = data[0] / 10.0;
//                 double humidity = data[1] / 10.0;
                
//                 influxDBService.saveSensorData(temperature, humidity, device.getHost(), device.getDeviceId());
//                 lastDataSaveTime.put(device.getDeviceId(), currentTime);
                
//                 log.debug("데이터 저장 완료 - deviceId: {}, temp: {}, humidity: {}", 
//                     device.getDeviceId(), temperature, humidity);
//             }

//             return data;
//         } catch (Exception e) {
//             log.error("데이터 읽기 실패: {}", device.getDeviceId(), e);
//             throw e;
//         }
//     }

//     private ModbusTCPMaster getOrCreateConnection(ModbusDevice device) throws Exception {
//         ModbusTCPMaster master = modbusMasters.get(device.getDeviceId());
//         if (master == null) {
//             log.info("장치 연결 실패: {}", device.getDeviceId());
//             throw new RuntimeException("ModbusTCPMaster not found for device: " + device.getDeviceId());
//         }
//         if (!master.isConnected()) {
//             log.info("재연결 시도: {}", device.getDeviceId());
//             master.connect();
//         }
//         return master;
//     }

//     public void removeDevice(String deviceId) {
//         ModbusTCPMaster master = modbusMasters.remove(deviceId);
//         if (master != null) {
//             try {
//                 master.disconnect();
//             } catch (Exception e) {
//                 log.error("장치 연결 해제 실패: {} - {}", deviceId, e.getMessage());
//             }
//         }
//         registeredDevices.removeIf(device -> device.getDeviceId().equals(deviceId));
//     }

//     public List<ModbusDevice> deviceList() {
//         try {
//             return modbusDeviceRepository.findAll()
//                     .stream()
//                     .filter(Objects::nonNull)
//                     .map(document -> {
//                         try {
//                             return document.toModbusDevice();
//                         } catch (Exception e) {
//                             log.error("디바이스 변환 실패: {} - {}", document.getDeviceId(), e.getMessage());
//                             return null;
//                         }
//                     })
//                     .filter(Objects::nonNull)
//                     .collect(Collectors.toList());
//         } catch (Exception e) {
//             log.error("디바이스 목록 조회 실패: {}", e.getMessage());
//             return Collections.emptyList();
//         }
//     }
//     public boolean deleteDevice(String deviceId) {
//         long result = modbusDeviceRepository.deleteByDeviceId(deviceId);
//         if(result > 0){
//             return true;
//         }
//         return false;
//     }

//     @PreDestroy
//     public void cleanup() {
//         modbusMasters.forEach((deviceId, master) -> {
//             try {
//                 master.disconnect();
//             } catch (Exception e) {
//                 log.error("연결 해제 실패: {}", deviceId, e);
//             }
//         });
//         modbusMasters.clear();
//         lastDataSaveTime.clear();
//     }
// }
package com.yth.realtime.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.Register;
import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.entity.ModbusDeviceDocument;
import com.yth.realtime.repository.ModbusDeviceRepository;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ModbusService {
    private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
    private final InfluxDBService influxDBService;
    private final List<ModbusDevice> registeredDevices = new CopyOnWriteArrayList<>();
    private final Map<String, ModbusTCPMaster> modbusMasters = new ConcurrentHashMap<>();
    private final Map<String, Long> lastDataSaveTime = new ConcurrentHashMap<>();
    private static final long SAVE_INTERVAL = 5; // 5초로 수정
    private final ModbusDeviceRepository modbusDeviceRepository;

    public ModbusService(InfluxDBService influxDBService, ModbusDeviceRepository modbusDeviceRepository) {
        this.influxDBService = influxDBService;
        this.modbusDeviceRepository = modbusDeviceRepository;
    }

    /**
     * 장치를 등록하는 메서드
     */
    public boolean addDevice(ModbusDevice device) {
        try {
           
            // 장치 연결 테스트
            ModbusTCPMaster master = new ModbusTCPMaster(device.getHost(), device.getPort());
            master.setTimeout(1000);
            master.setRetries(1);
            master.connect();  // 연결 시도
            
            // 테스트 읽기 수행
            Register[] testRead = master.readMultipleRegisters(
                device.getSlaveId(), 
                device.getStartAddress(), 
                device.getLength()
            );
            
            if (testRead != null && testRead.length > 0) {
                modbusMasters.put(device.getDeviceId(), master);
                registeredDevices.add(device);
                if(!modbusDeviceRepository.existsByDeviceId(device.getDeviceId())){
                    modbusDeviceRepository.save(ModbusDeviceDocument.from(device));
                }
                log.info("장치 연결 성공: {}", device);
                return true;
            } else {
                throw new RuntimeException("장치 응답 없음");
            }
        } catch (Exception e) {
            log.error("장치 연결 실패: {} - {}", device.getDeviceId(), e.getMessage());
            return false;
        }
    }

    /**
     * Modbus 데이터를 읽어서 InfluxDB에 저장하고 반환
     */
    public int[] readModbusData(ModbusDevice device) throws Exception {
        ModbusTCPMaster master = getOrCreateConnection(device);
        try {
            Register[] registers = master.readMultipleRegisters(
                device.getSlaveId(),
                device.getStartAddress(),
                device.getLength()
            );

            int[] data = new int[]{registers[0].getValue(), registers[1].getValue()};
            
            // 현재 시간을 초 단위로 가져옴
            long currentTime = System.currentTimeMillis() / 1000;
            Long lastSave = lastDataSaveTime.get(device.getDeviceId());
            
            // 마지막 저장으로부터 5초가 지났거나 처음 저장하는 경우에만 저장
            if (lastSave == null || currentTime - lastSave >= SAVE_INTERVAL) {
                double temperature = data[0] / 10.0;
                double humidity = data[1] / 10.0;
                
                try {
                    influxDBService.saveSensorData(temperature, humidity, device.getHost(), device.getDeviceId());
                    lastDataSaveTime.put(device.getDeviceId(), currentTime);
                    log.debug("데이터 저장 완료 - deviceId: {}, temp: {}, humidity: {}", 
                        device.getDeviceId(), temperature, humidity);
                } catch (Exception e) {
                    log.error("데이터 저장 실패: {} - {}", device.getDeviceId(), e.getMessage());
                }
            }

            return data;
        } catch (Exception e) {
            log.error("데이터 읽기 실패: {} - {}", device.getDeviceId(), e.getMessage());
            throw e;
        }
    }

    private ModbusTCPMaster getOrCreateConnection(ModbusDevice device) throws Exception {
        ModbusTCPMaster master = modbusMasters.get(device.getDeviceId());
        if (master == null || !master.isConnected()) {
            try {
                master = new ModbusTCPMaster(device.getHost(), device.getPort());
                master.setTimeout(1000);
                master.setRetries(1);
                master.connect();  // 예외 발생 가능
                modbusMasters.put(device.getDeviceId(), master);
                log.info("새로운 Modbus 연결 생성: {}", device.getDeviceId());
            } catch (Exception e) {
                log.error("Modbus 연결 실패: {} - {}", device.getDeviceId(), e.getMessage());
                throw e;  // 상위로 예외 전파
            }
        }
        return master;
    }

    public void removeDevice(String deviceId) {
        ModbusTCPMaster master = modbusMasters.remove(deviceId);
        if (master != null) {
            try {
                master.disconnect();
            } catch (Exception e) {
                log.error("장치 연결 해제 실패: {} - {}", deviceId, e.getMessage());
            }
        }
        registeredDevices.removeIf(device -> device.getDeviceId().equals(deviceId));
    }

    public List<ModbusDevice> deviceList() {
        try {
            return modbusDeviceRepository.findAll()
                    .stream()
                    .filter(Objects::nonNull)
                    .map(document -> {
                        try {
                            return document.toModbusDevice();
                        } catch (Exception e) {
                            log.error("디바이스 변환 실패: {} - {}", document.getDeviceId(), e.getMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("디바이스 목록 조회 실패: {}", e.getMessage());
            return Collections.emptyList();
        }
    }
    public boolean deleteDevice(String deviceId) {
        long result = modbusDeviceRepository.deleteByDeviceId(deviceId);
        if(result > 0){
            return true;
        }
        return false;
    }

    @PreDestroy
    public void cleanup() {
        modbusMasters.forEach((deviceId, master) -> {
            try {
                master.disconnect();
            } catch (Exception e) {
                log.error("연결 해제 실패: {}", deviceId, e);
            }
        });
        modbusMasters.clear();
        lastDataSaveTime.clear();
    }

    /**
     * 장치 존재 여부를 확인하는 메서드
     * @param deviceId 확인할 장치 ID
     * @return 장치 존재 여부
     */
    public boolean deviceExists(String deviceId) {
        return modbusMasters.containsKey(deviceId);
    }
}