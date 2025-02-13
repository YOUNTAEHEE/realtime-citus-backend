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

@Service
public class ModbusService {
    private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
    private final InfluxDBService influxDBService;
    private final List<ModbusDevice> registeredDevices = new CopyOnWriteArrayList<>();
    private final Map<String, ModbusTCPMaster> modbusMasters = new ConcurrentHashMap<>();
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
            master.setTimeout(5000);
            master.setRetries(3);
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
    public int[] readModbusData(ModbusDevice device) {
        ModbusTCPMaster master = modbusMasters.get(device.getDeviceId());
        
        if (master == null) {
            log.error("ModbusTCPMaster not found for device: {}", device.getDeviceId());
            return new int[]{0, 0};
        }

        try {
            if (!master.isConnected()) {
                log.info("재연결 시도: {}", device.getDeviceId());
                master.connect();
            }

            Register[] registers = master.readMultipleRegisters(
                device.getSlaveId(), 
                device.getStartAddress(), 
                device.getLength()
            );

            if (registers == null || registers.length == 0) {
                log.error("레지스터 읽기 실패: {}", device.getDeviceId());
                return new int[]{0, 0};
            }

            int[] data = new int[device.getLength()];
            for (int i = 0; i < device.getLength(); i++) {
                data[i] = registers[i].getValue();
            }

            // 데이터 저장
            double temperature = data[0] / 10.0;
            double humidity = data[1] / 10.0;
            influxDBService.saveSensorData(temperature, humidity, device.getHost(), device.getDeviceId());

            log.debug("데이터 읽기 성공 - deviceId: {}, temp: {}, humidity: {}", 
                device.getDeviceId(), temperature, humidity);

            return data;

        } catch (Exception e) {
            log.error("데이터 읽기 실패: {} - {}", device.getDeviceId(), e.getMessage());
            return new int[]{0, 0};
        }
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
}