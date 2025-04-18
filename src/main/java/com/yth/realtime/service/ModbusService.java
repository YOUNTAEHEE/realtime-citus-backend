package com.yth.realtime.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.Register;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.dto.SettingsDTO;
import com.yth.realtime.entity.ModbusDeviceDocument;
import com.yth.realtime.entity.Settings;
import com.yth.realtime.event.HistoricalDataEvent;
import com.yth.realtime.repository.ModbusDeviceRepository;
import com.yth.realtime.repository.SettingsRepository;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Transactional
@Slf4j
@EnableScheduling
@RequiredArgsConstructor
public class ModbusService {
    private static final Logger log = LoggerFactory.getLogger(ModbusService.class);
    private final InfluxDBService influxDBService;
    private final List<ModbusDevice> registeredDevices = new CopyOnWriteArrayList<>();
    private final Map<String, ModbusTCPMaster> modbusMasters = new ConcurrentHashMap<>();
    private final Map<String, Long> lastDataSaveTime = new ConcurrentHashMap<>();
    private static final long SAVE_INTERVAL = 1; // 5초로 수정
    private final ModbusDeviceRepository modbusDeviceRepository;
    private final SettingsRepository settingsRepository;

    // scheduler 변수 추가
    private ScheduledExecutorService scheduler;

    // ApplicationEventPublisher 추가 (이벤트 기반 통신 위해)
    private final ApplicationEventPublisher eventPublisher;
    // === 추가: CSV 삽입 시도 횟수 카운터 ===
    private final AtomicLong csvInsertCounter = new AtomicLong(0);
    // pointQueue 필드 선언 추가
    private final BlockingQueue<Point> pointQueue = new LinkedBlockingQueue<>();

    // === 카운터 유지 (반복 실행 모드에서 사용) ===
    private final AtomicLong dummyDataGenerationCounter = new AtomicLong(0);

    /**
     * 장치를 등록하는 메서드
     */
    public boolean addDevice(ModbusDevice device) {
        try {
            // 장치 연결 테스트
            ModbusTCPMaster master = new ModbusTCPMaster(device.getHost(), device.getPort());
            master.setTimeout(1000);
            master.setRetries(1);
            master.connect(); // 연결 시도

            // 테스트 읽기 수행
            Register[] testRead = master.readMultipleRegisters(
                    device.getSlaveId(),
                    device.getStartAddress(),
                    device.getLength());

            if (testRead != null && testRead.length > 0) {
                modbusMasters.put(device.getDeviceId(), master);
                registeredDevices.add(device);
                if (!modbusDeviceRepository.existsByDeviceId(device.getDeviceId())) {
                    modbusDeviceRepository.save(ModbusDeviceDocument.from(device));
                }
                log.info("장치 연결 성공: {}", device);

                // 스케줄러가 없으면 초기화
                if (scheduler == null || scheduler.isShutdown()) {
                    scheduler = Executors.newScheduledThreadPool(1);
                    log.info("모드버스 스케줄러 초기화");
                }

                // 장치 등록 성공 시 24시간 이전 데이터 전송
                sendHistoricalData(device.getDeviceId());
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
     * 24시간 이전 데이터 전송 메서드
     */
    private void sendHistoricalData(String deviceId) {
        try {
            log.info("24시간 이전 데이터 로드 시작: {}", deviceId);

            // InfluxDB에서 24시간 데이터 가져오기 (1440분 = 24시간)
            List<Map<String, Object>> historicalData = influxDBService.getRecentSensorData(deviceId, 1440);

            if (historicalData != null && !historicalData.isEmpty()) {
                // 데이터가 있으면 이벤트 발행
                Map<String, Object> message = new HashMap<>();
                message.put("type", "history");
                message.put("deviceId", deviceId);
                message.put("data", historicalData);

                // 이벤트 발행을 통한 WebSocketHandler와의 통신
                eventPublisher.publishEvent(new HistoricalDataEvent(this, deviceId, message));

                log.info("24시간 이전 데이터 전송 완료: {}개 데이터 포인트", historicalData.size());
            } else {
                log.info("24시간 이전 데이터가 없습니다: {}", deviceId);
            }
        } catch (Exception e) {
            log.error("24시간 이전 데이터 전송 중 오류 발생: {}", e.getMessage(), e);
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
                    device.getLength());

            int[] data = new int[] { registers[0].getValue(), registers[1].getValue() };

            // 현재 시간을 초 단위로 가져옴
            long currentTime = System.currentTimeMillis() / 1000;
            Long lastSave = lastDataSaveTime.get(device.getDeviceId());

            // 마지막 저장으로부터 5초가 지났거나 처음 저장하는 경우에만 저장
            if (lastSave == null || (currentTime - lastSave) >= SAVE_INTERVAL) {
                double temperature = data[0] / 10.0;
                double humidity = data[1] / 10.0;

                try {
                    influxDBService.saveSensorData(temperature, humidity, device.getHost(),
                            device.getDeviceId());
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
                master.connect(); // 예외 발생 가능
                modbusMasters.put(device.getDeviceId(), master);
                log.info("새로운 Modbus 연결 생성: {}", device.getDeviceId());
            } catch (Exception e) {
                log.error("Modbus 연결 실패: {} - {}", device.getDeviceId(), e.getMessage());
                throw e; // 상위로 예외 전파
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
        removeDevice(deviceId);// 추가
        long result = modbusDeviceRepository.deleteByDeviceId(deviceId);
        if (result > 0) {
            return true;
        }
        return false;
    }
//살리기
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
     * 
     * @param deviceId 확인할 장치 ID
     * @return 장치 존재 여부
     */
    public boolean deviceExists(String deviceId) {
        return modbusMasters.containsKey(deviceId);
    }

    public SettingsDTO getSettings() {
        // 온도 설정 조회
        Settings temperatureSettings = settingsRepository.getByType("temperature");
        // 습도 설정 조회
        Settings humiditySettings = settingsRepository.getByType("humidity");

        // DTO 생성
        SettingsDTO settingsDTO = new SettingsDTO();

        // 온도 설정 변환

        SettingsDTO.SensorSettingsDTO tempDTO = new SettingsDTO.SensorSettingsDTO();
        tempDTO.setWarningLow(temperatureSettings.getWarningLow());
        tempDTO.setDangerLow(temperatureSettings.getDangerLow());
        tempDTO.setNormal(temperatureSettings.getNormal());
        tempDTO.setWarningHigh(temperatureSettings.getWarningHigh());
        tempDTO.setDangerHigh(temperatureSettings.getDangerHigh());
        settingsDTO.setTemperature(tempDTO);
        // 습도 설정 변환
        SettingsDTO.SensorSettingsDTO humidDTO = new SettingsDTO.SensorSettingsDTO();
        humidDTO.setWarningLow(humiditySettings.getWarningLow());
        humidDTO.setDangerLow(humiditySettings.getDangerLow());
        humidDTO.setNormal(humiditySettings.getNormal());
        humidDTO.setWarningHigh(humiditySettings.getWarningHigh());
        humidDTO.setDangerHigh(humiditySettings.getDangerHigh());
        settingsDTO.setHumidity(humidDTO);
        System.out.println("변환된 설정 DTO: " + settingsDTO);
        return settingsDTO;
    }

    public void saveSettings(SettingsDTO settingsDTO) {
        // 온도 설정 저장
        Settings temperatureSettings = settingsRepository.getByType("temperature");
        if (temperatureSettings == null) {
            temperatureSettings = new Settings();
            temperatureSettings.setType("temperature");
        }

        temperatureSettings.setWarningLow(settingsDTO.getTemperature().getWarningLow());
        temperatureSettings.setDangerLow(settingsDTO.getTemperature().getDangerLow());
        temperatureSettings.setNormal(settingsDTO.getTemperature().getNormal());
        temperatureSettings.setWarningHigh(settingsDTO.getTemperature().getWarningHigh());
        temperatureSettings.setDangerHigh(settingsDTO.getTemperature().getDangerHigh());

        settingsRepository.save(temperatureSettings);

        // 습도 설정 저장
        Settings humiditySettings = settingsRepository.getByType("humidity");
        if (humiditySettings == null) {
            humiditySettings = new Settings();
            humiditySettings.setType("humidity");
        }

        humiditySettings.setWarningLow(settingsDTO.getHumidity().getWarningLow());
        humiditySettings.setDangerLow(settingsDTO.getHumidity().getDangerLow());
        humiditySettings.setNormal(settingsDTO.getHumidity().getNormal());
        humiditySettings.setWarningHigh(settingsDTO.getHumidity().getWarningHigh());
        humiditySettings.setDangerHigh(settingsDTO.getHumidity().getDangerHigh());

        settingsRepository.save(humiditySettings);
    }

    public void updateDevice(ModbusDevice deviceDTO) {
        log.info("장치 업데이트 요청: {}", deviceDTO);

        // 장치 ID로 모든 장치 조회해보기
        List<ModbusDeviceDocument> allDevices = modbusDeviceRepository.findAll();
        log.info("현재 등록된 모든 장치: {}", allDevices);

        // 기존 장치 조회
        try {
            ModbusDeviceDocument existingDevice = modbusDeviceRepository.findByDeviceId(deviceDTO.getDeviceId())
                    .orElseThrow(() -> new RuntimeException("장치를 찾을 수 없습니다: " + deviceDTO.getDeviceId()));

            // 장치 정보 업데이트
            existingDevice.setName(deviceDTO.getName());
            existingDevice.setHost(deviceDTO.getHost());
            existingDevice.setPort(deviceDTO.getPort());
            existingDevice.setSlaveId(deviceDTO.getSlaveId());
            existingDevice.setStartAddress(deviceDTO.getStartAddress());
            existingDevice.setLength(deviceDTO.getLength());

            // 장치 저장
            modbusDeviceRepository.save(existingDevice);
            log.info("장치 업데이트 성공: {}", existingDevice);

            // 기존 Modbus 연결 재설정
            resetConnection(deviceDTO.getDeviceId());

            // 등록된 장치 목록에서도 업데이트
            for (int i = 0; i < registeredDevices.size(); i++) {
                if (registeredDevices.get(i).getDeviceId().equals(deviceDTO.getDeviceId())) {
                    registeredDevices.set(i, deviceDTO);
                    break;
                }
            }

            // 새 연결 생성 시도
            try {
                ModbusTCPMaster master = new ModbusTCPMaster(deviceDTO.getHost(), deviceDTO.getPort());
                master.setTimeout(1000);
                master.setRetries(1);
                master.connect();
                modbusMasters.put(deviceDTO.getDeviceId(), master);
                log.info("장치 연결 재설정 성공: {}", deviceDTO.getDeviceId());
            } catch (Exception e) {
                log.error("장치 연결 재설정 실패: {} - {}", deviceDTO.getDeviceId(), e.getMessage());
                // 연결 실패해도 업데이트는 성공으로 처리
            }
        } catch (Exception e) {
            log.error("장치 업데이트 실패 상세 정보: ", e);
            throw e;
        }
    }

    public void resetConnection(String deviceId) {
        ModbusTCPMaster master = modbusMasters.remove(deviceId);
        if (master != null) {
            try {
                master.disconnect();
                log.info("Modbus 연결 재설정: {}", deviceId);
            } catch (Exception e) {
                log.error("Modbus 연결 해제 실패: {} - {}", deviceId, e.getMessage());
            }
        }
    }

    public ModbusDevice getDevice(String deviceId) {
        return modbusDeviceRepository.findByDeviceId(deviceId)
                .map(ModbusDeviceDocument::toModbusDevice)
                .orElse(null);
    }

    /**
     * 모드버스 서비스의 스케줄러를 중지하는 메서드
     * 실행 중인 모든 스케줄링 작업을 안전하게 종료합니다.
     */
    public void stopScheduler() {
        log.info("모드버스 스케줄러 중지 시작");

        try {
            // 기존 스케줄러가 있고 아직 종료되지 않았다면 종료
            if (scheduler != null && !scheduler.isShutdown()) {
                // 즉시 종료 요청 (실행 중인 작업도 중단)
                scheduler.shutdownNow();

                // 최대 5초간 모든 작업이 종료될 때까지 대기
                boolean terminated = scheduler.awaitTermination(5, TimeUnit.SECONDS);

                if (terminated) {
                    log.info("모드버스 스케줄러가 정상적으로 종료되었습니다.");
                } else {
                    log.warn("모드버스 스케줄러 종료 타임아웃: 일부 작업이 여전히 실행 중일 수 있습니다.");
                }

                // 스케줄러 참조 제거
                scheduler = null;
            } else {
                log.info("종료할 모드버스 스케줄러가 없거나 이미 종료되었습니다.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 인터럽트 상태 복원
            log.error("모드버스 스케줄러 종료 중 인터럽트 발생", e);
        } catch (Exception e) {
            log.error("모드버스 스케줄러 종료 중 예외 발생", e);
        }

        log.info("모드버스 스케줄러 중지 완료");
    }

    /**
     * 모든 Modbus 장치 연결을 종료하는 메서드
     */
    public void disconnectAllDevices() {
        log.info("모든 Modbus 장치 연결 종료 시작");

        // 스케줄러 종료
        // stopScheduler();

        modbusMasters.forEach((deviceId, master) -> {
            try {
                master.disconnect();
                log.info("Modbus 장치 연결 종료 성공: {}", deviceId);
            } catch (Exception e) {
                log.error("Modbus 장치 연결 종료 실패: {} - {}", deviceId, e.getMessage(), e);
            }
        });

        // 연결 객체 맵 초기화
        modbusMasters.clear();

        log.info("모든 Modbus 장치 연결 종료 완료");
    }

}
