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
    // @PreDestroy
    // public void cleanup() {
    //     modbusMasters.forEach((deviceId, master) -> {
    //         try {
    //             master.disconnect();
    //         } catch (Exception e) {
    //             log.error("연결 해제 실패: {}", deviceId, e);
    //         }
    //     });
    //     modbusMasters.clear();
    //     lastDataSaveTime.clear();
    // }

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

    // // ========================================================================
    // // === Helper Classes for In-Memory Structure ===
    // // ========================================================================

    // @Getter // Lombok 사용 시, 없으면 직접 Getter 추가
    // static class Rack {
    //     private final int rackId;
    //     private final List<Module> modules = new ArrayList<>();

    //     public Rack(int rackId) {
    //         this.rackId = rackId;
    //     }

    //     public void addModule(Module module) {
    //         this.modules.add(module);
    //     }

    //     public int getRackId() {
    //         return rackId;
    //     } // Lombok 미사용 시

    //     public List<Module> getModules() {
    //         return modules;
    //     } // Lombok 미사용 시
    // }

    // @Getter // Lombok 사용 시, 없으면 직접 Getter 추가
    // static class Module {
    //     private final int moduleId;
    //     private final List<Cell> cells = new ArrayList<>();

    //     public Module(int moduleId) {
    //         this.moduleId = moduleId;
    //     }

    //     public void addCell(Cell cell) {
    //         this.cells.add(cell);
    //     }

    //     public int getModuleId() {
    //         return moduleId;
    //     } // Lombok 미사용 시

    //     public List<Cell> getCells() {
    //         return cells;
    //     } // Lombok 미사용 시
    // }

    // @Getter // Lombok 사용 시, 없으면 직접 Getter 추가
    // static class Cell {
    //     private final int cellId;
    //     private final List<Double> values; // 12개의 값 저장

    //     public Cell(int cellId, List<Double> values) {
    //         this.cellId = cellId;
    //         this.values = values;
    //     }

    //     public int getCellId() {
    //         return cellId;
    //     } // Lombok 미사용 시

    //     public List<Double> getValues() {
    //         return values;
    //     } // Lombok 미사용 시
    // }

    // // ========================================================================
    // // === 실행 모드 및 저장 방식 선택 ===
    // // ========================================================================

    // // @PostConstruct // <<< 모드 1: 한 번 실행 (활성화하려면 주석 해제)
    // public void runOnceOnStartup() {
    //     log.info("===== @PostConstruct: 더미 데이터 1회 생성 및 저장 시작 =====");
    //     // --- 저장 방식 선택 (아래 두 줄 중 하나만 주석 해제) ---
    //     // generateAndSaveInBatches(1); // 권장: 메모리 리스트 생성 후 배치 저장 (5434 행)
    //     // generateAndSaveAllAtOnce(1); // 주의: 메모리 리스트 생성 후 한번에 저장 (5434 행)
    // }

    // @Scheduled(fixedRate = 1000) // <<< 모드 2: 반복 실행 (활성화하려면 주석 해제)
    // public void repeatDummyDataGeneration() {
    //     long currentAttempt = dummyDataGenerationCounter.incrementAndGet();
    //     log.info("===== @Scheduled: 더미 데이터 반복 생성 및 저장 시작 (시도 #{}) =====", currentAttempt);
    //     // --- 저장 방식 선택 (아래 두 줄 중 하나만 주석 해제) ---
    //     generateAndSaveInBatches(currentAttempt); // 권장: 메모리 리스트 생성 후 배치 저장 (5434 행)
    //     // generateAndSaveAllAtOnce(currentAttempt); // 주의: 메모리 리스트 생성 후 한번에 저장 (5434 행)
    // }

    // // 소수점 3자리 반올림 헬퍼
    // private double round3(double value) {
    //     return Math.round(value * 1000.0) / 1000.0;
    // }

    // // 소수점 2자리 반올림 헬퍼 (필요 시 사용)
    // private double round2(double value) {
    //     return Math.round(value * 100.0) / 100.0;
    // }

    // // ========================================================================
    // // === 저장 방식 1: 메모리 리스트 생성 -> 배치 저장 (5434 행) ===
    // // ========================================================================
    // /**
    //  * 1. Rack-Module-Cell(12 values) 구조를 메모리에 생성.
    //  * 2. 생성된 리스트를 순회하며 각 Cell당 하나의 Point(행) 생성 (value_0 ~ value_11 필드 포함).
    //  * 3. 각 Rack 처리가 끝날 때마다 해당 Rack의 Point(418개)를 배치로 InfluxDB에 저장합니다.
    //  * 총 5,434 행 생성 (13 * 19 * 22), 13번의 저장 호출 발생.
    //  *
    //  * @param attemptCount 현재 시도 횟수
    //  */
    // public void generateAndSaveInBatches(long attemptCount) {
    //     long processStartTime = System.currentTimeMillis();
    //     final int NUM_RACKS = 13;
    //     final int NUM_MODULES = 19;
    //     final int NUM_CELLS = 22;
    //     final int NUM_VALUES_PER_CELL = 12;
    //     final String MEASUREMENT_NAME = "dummy_cell_values_row_v1_test10";

    //     List<Point> batch = new ArrayList<>();
    //     long totalPointsGenerated = 0;
    //     long totalPointsSaved = 0;
    //     Random random = new Random();
    //     log.info("[Attempt #{}] 더미 데이터 생성(리스트) 및 <랙 단위 배치 저장> 시작 (목표 행: {})...", attemptCount,
    //             (long) NUM_RACKS * NUM_MODULES * NUM_CELLS);

    //     try {
    //         // === 1단계: 메모리에 List<Rack> 구조 생성 ===
    //         long listGenStartTime = System.currentTimeMillis();
    //         List<Rack> rackList = new ArrayList<>();
    //         for (int rackId = 1; rackId <= NUM_RACKS; rackId++) {
    //             Rack rack = new Rack(rackId);
    //             for (int moduleId = 1; moduleId <= NUM_MODULES; moduleId++) {
    //                 Module module = new Module(moduleId);
    //                 for (int cellId = 1; cellId <= NUM_CELLS; cellId++) {
    //                     List<Double> values = new ArrayList<>();
    //                     for (int i = 0; i < NUM_VALUES_PER_CELL; i++) {
    //                         values.add(round3(3.0 + random.nextDouble() * (4.2 - 3.0)));
    //                     }
    //                     Cell cell = new Cell(cellId, values);
    //                     module.addCell(cell);
    //                 }
    //                 rack.addModule(module);
    //             }
    //             rackList.add(rack);
    //         }
    //         long listGenEndTime = System.currentTimeMillis();
    //         log.info("[Attempt #{}] 메모리 리스트 구조 생성 완료 ({} ms)", attemptCount, listGenEndTime - listGenStartTime);

    //         // === 2단계: 생성된 List<Rack> 순회하며 Point 생성 및 <랙 단위> 저장 ===
    //         Instant timestamp = Instant.now();

    //         processLoop: for (Rack rack : rackList) {
    //             // === 각 랙 시작 시 배치를 비움 ===
    //             batch.clear();
    //             log.trace("[Attempt #{}] Processing Rack #{}", attemptCount, rack.getRackId());

    //             for (Module module : rack.getModules()) {
    //                 for (Cell cell : module.getCells()) {
    //                     if (Thread.currentThread().isInterrupted()) {
    //                         break processLoop;
    //                     }
    //                     totalPointsGenerated++;

    //                     try {
    //                         // === Point 생성 로직 ===
    //                         Point point = Point.measurement(MEASUREMENT_NAME)
    //                                 .addTag("rack_id", String.valueOf(rack.getRackId()))
    //                                 .addTag("module_id", String.valueOf(module.getModuleId()))
    //                                 .addTag("cell_id", String.valueOf(cell.getCellId()));

    //                         List<Double> cellValues = cell.getValues();
    //                         for (int i = 0; i < cellValues.size(); i++) {
    //                             point = point.addField("value_" + i, cellValues.get(i));
    //                         }
    //                         point = point.time(timestamp, WritePrecision.MS);

    //                         batch.add(point); // 현재 랙의 배치에 추가

    //                     } catch (Exception e) {
    //                         log.warn("[Attempt #{}] Point 생성 또는 배치 추가 중 오류 발생: R{} M{} C{}",
    //                                 attemptCount, rack.getRackId(), module.getModuleId(), cell.getCellId(), e);
    //                     }
    //                 } // cell loop
    //             } // module loop

    //             // === 랙 처리 완료 후: 해당 랙의 배치 저장 ===
    //             if (!batch.isEmpty()) {
    //                 log.info("[Attempt #{}] Saving batch for Rack #{} ({} points)...", attemptCount, rack.getRackId(),
    //                         batch.size());
    //                 saveBatch(batch, attemptCount); // 해당 랙의 포인트(418개) 저장
    //                 totalPointsSaved += batch.size();
    //             }
    //         } // rack loop

    //     } catch (Exception e) {
    //         log.error("[Attempt #{}] 더미 데이터 생성/저장 프로세스 중 예외 발생", attemptCount, e);
    //     } finally {
    //         long processEndTime = System.currentTimeMillis();
    //         long duration = processEndTime - processStartTime;
    //         log.info("===== 더미 데이터 생성(리스트) 및 <랙 단위 배치 저장> 완료 (시도 #{}) =====", attemptCount);
    //         log.info("총 생성된 포인트(행): {}, 실제 DB 저장된 포인트: {}", totalPointsGenerated, totalPointsSaved);
    //         log.info("총 소요 시간: {} ms", duration);
    //         if (duration <= 1000) {
    //             log.info(">>>> 성공: 1초 목표 달성 (시도 #{})", attemptCount);
    //         } else {
    //             log.warn("<<<< 경고: 1초 목표 초과 ({} ms, 시도 #{})", duration, attemptCount);
    //         }
    //     }
    // }

    // // ========================================================================
    // // === 저장 방식 2: 메모리 리스트 생성 -> 한 번에 저장 (5434 행) ===
    // // ========================================================================
    // /**
    //  * 1. Rack-Module-Cell(12 values) 구조를 메모리에 생성.
    //  * 2. 생성된 리스트를 순회하며 각 Cell당 하나의 Point(행) 생성 (value_0 ~ value_11 필드 포함).
    //  * 3. 생성된 모든 Point(5,434개)를 InfluxDB에 한 번에 저장합니다.
    //  * 주의: DB 부하 및 타임아웃 가능성이 있습니다.
    //  *
    //  * @param attemptCount 현재 시도 횟수
    //  */
    // public void generateAndSaveAllAtOnce(long attemptCount) {
    //     long processStartTime = System.currentTimeMillis();
    //     final int NUM_RACKS = 13;
    //     final int NUM_MODULES = 19;
    //     final int NUM_CELLS = 22; // <<< 셀 개수 22개 확인
    //     final int NUM_VALUES_PER_CELL = 12; // 셀 당 값 개수
    //     final String MEASUREMENT_NAME = "dummy_cell_values_row_v1_test9"; // <<< 새 측정값 이름

    //     List<Point> allPoints = new ArrayList<>((int) (NUM_RACKS * NUM_MODULES * NUM_CELLS)); // 예상 크기 지정
    //     long totalPointsGenerated = 0; // 생성된 Point 객체 수 (행 수)
    //     Random random = new Random();
    //     log.warn("[Attempt #{}] 더미 데이터 생성(리스트) 및 <한 번에 저장> 시작 (목표 행: {})...", attemptCount,
    //             (long) NUM_RACKS * NUM_MODULES * NUM_CELLS); // <<< 5434

    //     try {
    //         // === 1단계: 메모리에 List<Rack> 구조 생성 ===
    //         long listGenStartTime = System.currentTimeMillis();
    //         List<Rack> rackList = new ArrayList<>();
    //         for (int rackId = 1; rackId <= NUM_RACKS; rackId++) {
    //             Rack rack = new Rack(rackId);
    //             for (int moduleId = 1; moduleId <= NUM_MODULES; moduleId++) {
    //                 Module module = new Module(moduleId);
    //                 for (int cellId = 1; cellId <= NUM_CELLS; cellId++) {
    //                     List<Double> values = new ArrayList<>();
    //                     for (int i = 0; i < NUM_VALUES_PER_CELL; i++) {
    //                         values.add(round3(3.0 + random.nextDouble() * (4.2 - 3.0))); // 3.0 ~ 4.2
    //                     }
    //                     Cell cell = new Cell(cellId, values);
    //                     module.addCell(cell);
    //                 }
    //                 rack.addModule(module);
    //             }
    //             rackList.add(rack);
    //         }
    //         long listGenEndTime = System.currentTimeMillis();
    //         log.info("[Attempt #{}] 메모리 리스트 구조 생성 완료 ({} ms)", attemptCount, listGenEndTime - listGenStartTime);

    //         // === 2단계: 생성된 List<Rack> 순회하며 Point 생성 ===
    //         Instant timestamp = Instant.now(); // 모든 포인트에 동일한 타임스탬프 적용

    //         processLoop: // 라벨 추가
    //         for (Rack rack : rackList) {
    //             for (Module module : rack.getModules()) {
    //                 for (Cell cell : module.getCells()) {
    //                     if (Thread.currentThread().isInterrupted()) {
    //                         break processLoop;
    //                     }

    //                     totalPointsGenerated++; // 셀 당 1 증가 (총 5434번)

    //                     try {
    //                         // === 각 Cell 객체당 하나의 Point 생성 ===
    //                         Point point = Point.measurement(MEASUREMENT_NAME)
    //                                 .addTag("rack_id", String.valueOf(rack.getRackId()))
    //                                 .addTag("module_id", String.valueOf(module.getModuleId()))
    //                                 .addTag("cell_id", String.valueOf(cell.getCellId()));

    //                         // === Cell의 12개 값을 value_0 ~ value_11 필드로 추가 ===
    //                         List<Double> cellValues = cell.getValues();
    //                         for (int i = 0; i < cellValues.size(); i++) {
    //                             point = point.addField("value_" + i, cellValues.get(i));
    //                         }

    //                         point = point.time(timestamp, WritePrecision.MS); // 타임스탬프 설정
    //                         allPoints.add(point); // 전체 리스트에 추가

    //                     } catch (Exception e) {
    //                         log.warn("[Attempt #{}] Point 생성 또는 리스트 추가 중 오류 발생: R{} M{} C{}",
    //                                 attemptCount, rack.getRackId(), module.getModuleId(), cell.getCellId(), e);
    //                     }
    //                 } // cell loop
    //             } // module loop
    //         } // rack loop

    //         // === 3단계: 모든 Point 한 번에 저장 ===
    //         if (!allPoints.isEmpty()) {
    //             log.warn("[Attempt #{}] 모든 더미 데이터({}) 생성 완료. DB에 <한 번에 저장> 시작...", attemptCount, allPoints.size());
    //             saveBatch(allPoints, attemptCount); // saveBatch 재사용 (이름은 그대로 두지만 실제론 전체 저장)
    //         } else {
    //             log.warn("[Attempt #{}] 생성된 더미 데이터가 없습니다.", attemptCount);
    //         }

    //     } catch (Exception e) {
    //         log.error("[Attempt #{}] 더미 데이터 생성/저장 프로세스 중 예외 발생", attemptCount, e);
    //     } finally {
    //         long processEndTime = System.currentTimeMillis();
    //         long duration = processEndTime - processStartTime;
    //         log.info("===== 더미 데이터 생성(리스트) 및 <한 번에 저장> 완료 (시도 #{}) =====", attemptCount);
    //         log.info("총 생성된 포인트(행): {}", totalPointsGenerated); // 5434개 목표
    //         log.info("총 소요 시간: {} ms", duration);
    //         if (duration <= 1000) {
    //             log.info(">>>> 성공: 1초 목표 달성 (시도 #{})", attemptCount);
    //         } else {
    //             log.warn("<<<< 경고: 1초 목표 초과 ({} ms, 시도 #{})", duration, attemptCount);
    //         }
    //     }
    // }

    // // ========================================================================
    // // === 저장 헬퍼 메서드 ===
    // // ========================================================================
    // /**
    //  * 제공된 포인트 리스트(배치 또는 전체)를 InfluxDB에 저장합니다.
    //  * 
    //  * @param pointsToSave 저장할 포인트 리스트
    //  * @param attemptCount 현재 시도 횟수 (로깅용)
    //  */
    // // private void saveBatch(List<Point> pointsToSave, long attemptCount) {
    // //     if (pointsToSave == null || pointsToSave.isEmpty()) {
    // //         return;
    // //     }
    // //     log.debug("[Attempt #{}] saveBatch 호출됨 - 포인트 {}개 저장 시도", attemptCount, pointsToSave.size());
    // //     try {
    // //         // influxDBService.savePoints(pointsToSave); // 원본 리스트 직접 전달 시 문제 발생 가능성?
    // //         influxDBService.savePoints(new ArrayList<>(pointsToSave)); // 방어적 복사본 전달
    // //     } catch (Exception e) {
    // //         log.error("!!! [Attempt #{}] InfluxDB 저장 중 심각한 오류 발생 (포인트 {}개).",
    // //                 attemptCount, pointsToSave.size(), e);
    // //         // 필요 시, 여기에 추가적인 오류 처리 로직 (예: 재시도, 실패 로깅 등)
    // //     }
    // // }
    // private void saveBatch(List<Point> pointsToSave, long attemptCount) {
    //     if (pointsToSave == null || pointsToSave.isEmpty()) {
    //         return;
    //     }
    
    //     final int THREAD_COUNT = 4; // 병렬 처리할 스레드 수
    //     final int BATCH_SIZE = 500; // 한 스레드가 처리할 포인트 수
    
    //     ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    
    //     int totalPoints = pointsToSave.size();
    //     log.debug("[Attempt #{}] 병렬 저장 시작 (총 {} 포인트)", attemptCount, totalPoints);
    
    //     for (int i = 0; i < totalPoints; i += BATCH_SIZE) {
    //         int start = i;
    //         int end = Math.min(i + BATCH_SIZE, totalPoints);
    //         List<Point> subBatch = new ArrayList<>(pointsToSave.subList(start, end));
    
    //         executor.submit(() -> {
    //             try {
    //                 influxDBService.savePoints(subBatch);
    //                 log.debug("[Attempt #{}] Sub-batch 저장 성공: {} ~ {}", attemptCount, start, end);
    //             } catch (Exception e) {
    //                 log.error("[Attempt #{}] Sub-batch 저장 실패 ({} ~ {}): {}", attemptCount, start, end, e.getMessage());
    //             }
    //         });
    //     }
    
    //     executor.shutdown();
    //     try {
    //         boolean finished = executor.awaitTermination(30, TimeUnit.SECONDS);
    //         if (!finished) {
    //             log.warn("[Attempt #{}] 일부 병렬 저장 작업이 타임아웃됨", attemptCount);
    //         } else {
    //             log.debug("[Attempt #{}] 모든 병렬 저장 작업 완료", attemptCount);
    //         }
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //         log.error("[Attempt #{}] 저장 쓰레드 인터럽트됨", attemptCount, e);
    //     }
    // }
    
// === 추가: 더미 데이터 처리용 큐 및 실행기 ===
private final BlockingQueue<Point> dummyDataQueue = new LinkedBlockingQueue<>(10000); // 큐 크기 제한
private ExecutorService dbSaverExecutor;
private final AtomicBoolean consuming = new AtomicBoolean(true); // 소비자 스레드 실행 플래그
private static final int DB_SAVE_BATCH_SIZE = 500; // DB 저장 배치 크기
private static final long DB_SAVE_INTERVAL_MS = 200; // 최대 배치 대기 시간
private final AtomicLong totalPointsSavedCounter = new AtomicLong(0); // 누적 저장 카운터 추가
private final AtomicLong loggedMilestoneCounter = new AtomicLong(0); // 로그 마일스톤 카운터 추가
private static final long POINTS_PER_GENERATION = 5434; // 생성 주기당 포인트 수

// === 추가: 스케줄러 필드 ===
// private ScheduledExecutorService scheduler;


@PostConstruct
public void initialize() {
    // ... (기존 초기화 로직 유지) ...
    log.info("ModbusService 초기화 시작...");

    consuming.set(true);
    dbSaverExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "dummy-db-saver");
        t.setDaemon(true);
        return t;
    });
    dbSaverExecutor.submit(this::consumeAndSaveDummyData);
    log.info("더미 데이터 DB 저장 소비자 스레드 시작됨.");

    log.info("ModbusService 초기화 완료.");
}


@PreDestroy
public void cleanup() {
   // ... (기존 cleanup 로직 시작 부분 유지) ...
    log.info("ModbusService 정리 시작...");

    log.info("더미 데이터 DB 저장 소비자 스레드 종료 요청...");
    consuming.set(false);
    if (dbSaverExecutor != null) {
        dbSaverExecutor.shutdown();
        try {
             // awaitTermination 전에 남은 데이터 처리
             log.info("종료 전 마지막 큐 데이터 처리 시도 (awaitTermination 전)...");
             drainQueueAndSaveRemaining(); // 수정: awaitTermination 전에 호출

            if (!dbSaverExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("DB 저장 소비자 스레드가 60초 내에 완전히 종료되지 않았습니다. 강제 종료 시도...");
                dbSaverExecutor.shutdownNow();
                // 강제 종료 후에도 혹시 모를 잔여 데이터 처리 시도 (필요 시)
                // log.info("강제 종료 후 마지막 큐 데이터 처리 시도...");
                // drainQueueAndSaveRemaining();
                if (!dbSaverExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                     log.error("DB 저장 소비자 스레드 강제 종료 실패.");
                }
            } else {
                 // 정상 종료 시에도 마지막 확인 (drainQueueAndSaveRemaining 호출이 이미 위에서 수행됨)
                log.info("DB 저장 소비자 스레드가 정상적으로 종료되었습니다.");
            }
        } catch (InterruptedException e) {
            log.error("DB 저장 소비자 스레드 종료 대기 중 인터럽트 발생.", e);
            dbSaverExecutor.shutdownNow();
            // 인터럽트 시에도 잔여 데이터 처리 시도
            log.info("인터럽트 발생. 종료 전 마지막 큐 데이터 처리 시도...");
            drainQueueAndSaveRemaining();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
             log.error("종료 중 큐 데이터 처리 실패", e);
        }
    }

    // ... (기존 Modbus, 스케줄러 종료 로직 유지) ...
     disconnectAllDevices();
     // stopScheduler();

    log.info("ModbusService 정리 완료. 총 저장된 포인트: {}", totalPointsSavedCounter.get()); // 최종 카운트 로깅
}

// === 수정: 종료 시 큐 잔여 데이터 처리 ===
private void drainQueueAndSaveRemaining() {
    log.debug("큐 드레이닝 시작...");
    List<Point> remainingPoints = new ArrayList<>();
    dummyDataQueue.drainTo(remainingPoints); // 큐의 모든 요소를 꺼내옴
    if (!remainingPoints.isEmpty()) {
        log.info("애플리케이션 종료 전 큐에 남은 데이터 {}개를 저장합니다.", remainingPoints.size());
        // 남은 데이터를 배치로 나누어 저장 (DB 부하 방지)
        for (int i = 0; i < remainingPoints.size(); i += DB_SAVE_BATCH_SIZE) {
            int end = Math.min(i + DB_SAVE_BATCH_SIZE, remainingPoints.size());
            List<Point> batch = remainingPoints.subList(i, end);
             if (!batch.isEmpty()) {
                saveBatchAndLogMilestone(new ArrayList<>(batch)); // 마일스톤 로깅 포함 버전 호출
             }
        }
    }
     log.debug("큐 드레이닝 완료.");
}


// ========================================================================
// === 더미 데이터 생성/저장 로직 (리팩토링됨) ===
// ========================================================================

// === 소비자: 큐에서 데이터를 가져와 DB에 저장 ===
private void consumeAndSaveDummyData() {
    log.info("DB 저장 소비자 스레드 실행 시작.");
    List<Point> batch = new ArrayList<>(DB_SAVE_BATCH_SIZE);
    long lastSaveTime = System.currentTimeMillis();

    while (consuming.get() || !dummyDataQueue.isEmpty()) { // 종료 신호 후에도 큐가 빌 때까지 계속
        try {
            Point point = dummyDataQueue.poll(100, TimeUnit.MILLISECONDS);

            if (point != null) {
                batch.add(point);
            }

            long now = System.currentTimeMillis();
            boolean shouldSave = !batch.isEmpty() &&
                                 (batch.size() >= DB_SAVE_BATCH_SIZE ||
                                  (now - lastSaveTime >= DB_SAVE_INTERVAL_MS));

            if (shouldSave) {
                log.debug("배치 저장 조건 충족 (크기: {}, 시간 경과: {}ms). 저장 시도...", batch.size(), now - lastSaveTime);
                saveBatchAndLogMilestone(new ArrayList<>(batch)); // 수정: 마일스톤 로깅 포함 버전 호출
                batch.clear();
                lastSaveTime = System.currentTimeMillis();
            }

            // 종료 시나리오: 종료 플래그 설정되고 큐가 비면 루프 탈출 (위의 while 조건에서 처리)

        } catch (InterruptedException e) {
            log.warn("DB 저장 소비자 스레드 인터럽트 발생. 종료 절차 진행...");
            Thread.currentThread().interrupt();
            consuming.set(false); // 안전 종료 플래그
        } catch (Exception e) {
            log.error("DB 저장 소비자 스레드 실행 중 예외 발생", e);
             // 오류 발생 시 현재 배치 데이터 유실 방지 위해 clear() 주석 처리 또는 다른 정책 적용
             // batch.clear();
        }
    }

     // 루프 종료 후 마지막 남은 배치 처리 (종료 시 drainQueueAndSaveRemaining 에서 처리하므로 중복될 수 있음 -> cleanup 로직에서만 처리하도록 변경)
    // if (!batch.isEmpty()) {
    //     log.info("소비자 스레드 종료 전 마지막 배치 {}개 저장.", batch.size());
    //     saveBatchAndLogMilestone(new ArrayList<>(batch));
    // }
    log.info("DB 저장 소비자 스레드 실행 종료. 총 저장된 포인트: {}", totalPointsSavedCounter.get());
}

// === 생산자: 더미 데이터를 생성하여 큐에 넣음 ===
@Scheduled(fixedRate = 1000)
public void repeatDummyDataGeneration() {
    // ... (기존 로직 동일) ...
    long currentAttempt = dummyDataGenerationCounter.incrementAndGet();
    try {
        generateAndQueueDummyData(currentAttempt);
    } catch (Exception e) {
        log.error("[Attempt #{}] 더미 데이터 생성 및 큐잉 중 예외 발생", currentAttempt, e);
    }
}

/**
 * 더미 데이터를 생성하여 BlockingQueue에 넣습니다.
 * @param attemptCount 현재 시도 횟수 (로깅용)
 */
public void generateAndQueueDummyData(long attemptCount) {
    // ... (기존 로직 동일, MEASUREMENT_NAME 확인/변경) ...
    long processStartTime = System.currentTimeMillis();
    final int NUM_RACKS = 13;
    final int NUM_MODULES = 19;
    final int NUM_CELLS = 22;
    final int NUM_VALUES_PER_CELL = 12;
    final String MEASUREMENT_NAME = "dummy_cell_values_row_v1_test14"; // <--- 측정값 이름 확인!
    final long TOTAL_POINTS_EXPECTED = (long) NUM_RACKS * NUM_MODULES * NUM_CELLS; // 5434

    long totalPointsGenerated = 0;
    Random random = new Random();
    log.info("[Attempt #{}] 더미 데이터 생성 및 <큐 저장> 시작 (목표 행: {})...", attemptCount, TOTAL_POINTS_EXPECTED);

    try {
        Instant timestamp = Instant.now();

        generationLoop:
        for (int rackId = 1; rackId <= NUM_RACKS; rackId++) {
             for (int moduleId = 1; moduleId <= NUM_MODULES; moduleId++) {
                 for (int cellId = 1; cellId <= NUM_CELLS; cellId++) {
                     if (Thread.currentThread().isInterrupted() || !consuming.get()) {
                        log.warn("[Attempt #{}] 데이터 생성 중 중단 요청 감지.", attemptCount);
                        break generationLoop;
                     }

                    try {
                        Point point = Point.measurement(MEASUREMENT_NAME)
                                .addTag("rack_id", String.valueOf(rackId))
                                .addTag("module_id", String.valueOf(moduleId))
                                .addTag("cell_id", String.valueOf(cellId));

                        List<Double> values = new ArrayList<>(NUM_VALUES_PER_CELL);
                        for (int i = 0; i < NUM_VALUES_PER_CELL; i++) {
                            values.add(round3(3.0 + random.nextDouble() * (4.2 - 3.0)));
                        }

                        for (int i = 0; i < values.size(); i++) {
                            point = point.addField("value_" + i, values.get(i));
                        }
                        point = point.time(timestamp, WritePrecision.MS);

                        boolean offered = dummyDataQueue.offer(point, 50, TimeUnit.MILLISECONDS);
                        if (offered) {
                            totalPointsGenerated++;
                        } else {
                            log.warn("[Attempt #{}] 큐가 가득 차서 Point 추가 실패 (Timeout). R{} M{} C{}.",
                                     attemptCount, rackId, moduleId, cellId);
                             // break generationLoop; // 필요 시 생성 중단
                        }

                    } catch (InterruptedException e) {
                         log.warn("[Attempt #{}] Point 큐 추가 대기 중 인터럽트 발생. 생성 중단.", attemptCount);
                         Thread.currentThread().interrupt();
                         break generationLoop;
                    } catch (Exception e) {
                        log.warn("[Attempt #{}] Point 생성 또는 큐 추가 중 오류 발생: R{} M{} C{}",
                                attemptCount, rackId, moduleId, cellId, e);
                    }
                }
            }
        }

    } finally {
        // ... (기존 finally 로직 동일) ...
        long processEndTime = System.currentTimeMillis();
        long duration = processEndTime - processStartTime;
        log.info("===== 더미 데이터 생성 및 <큐 저장> 완료 (시도 #{}) =====", attemptCount);
        log.info("총 생성 및 큐에 추가된 포인트(행): {} / {}", totalPointsGenerated, TOTAL_POINTS_EXPECTED);
        log.info("현재 큐 크기: {}", dummyDataQueue.size());
        log.info("총 생성 소요 시간: {} ms", duration);
         if (duration > 1000) {
            log.warn("<<<< 생성 시간 경고: 1초 목표 초과 ({} ms, 시도 #{})", duration, attemptCount);
        }
         if (totalPointsGenerated < TOTAL_POINTS_EXPECTED) {
             log.warn("<<<< 생성 개수 부족: 목표 {}개 중 {}개만 큐에 추가됨 (시도 #{})", TOTAL_POINTS_EXPECTED, totalPointsGenerated, attemptCount);
         }
    }
}


// === 더미 데이터 Helper Classes (Rack, Module, Cell) ===
// ... (기존 Helper Classes 동일) ...
 @Getter static class Rack { /* ... */ }
 @Getter static class Module { /* ... */ }
 @Getter static class Cell { /* ... */ }

// === 유틸리티 메서드 ===
private double round3(double value) {
    return Math.round(value * 1000.0) / 1000.0;
}


// === 저장 헬퍼 메서드 (내부 병렬 처리 제거 및 마일스톤 로깅 추가) ===
/**
 * 제공된 포인트 리스트(배치)를 InfluxDB에 저장하고,
 * 성공 시 누적 카운터를 업데이트하며 5434개 단위 마일스톤 로그를 남깁니다.
 * @param pointsToSave 저장할 포인트 리스트 (방어적 복사본으로 전달됨)
 */
private void saveBatchAndLogMilestone(List<Point> pointsToSave) {
    if (pointsToSave == null || pointsToSave.isEmpty()) {
        return;
    }

    int pointsInBatch = pointsToSave.size();
    log.debug("DB 저장 시작 ({} 포인트)...", pointsInBatch);
    long saveStartTime = System.currentTimeMillis();
    boolean success = false;

    try {
        influxDBService.savePoints(pointsToSave); // 직접 저장 시도
        success = true;
        long saveEndTime = System.currentTimeMillis();
        log.debug("DB 저장 성공 ({} 포인트, 소요 시간: {} ms)", pointsInBatch, saveEndTime - saveStartTime);

    } catch (Exception e) {
        long saveEndTime = System.currentTimeMillis();
        log.error("DB 저장 실패 ({} 포인트, 소요 시간: {} ms): {}", pointsInBatch, saveEndTime - saveStartTime, e.getMessage(), e);
         // 실패 시 재시도 로직 또는 실패 데이터 처리 로직 추가 가능
    }

    // 저장 성공 시 카운터 업데이트 및 로깅
    if (success) {
        long previousTotal = totalPointsSavedCounter.get(); // 마일스톤 확인용
        long currentTotal = totalPointsSavedCounter.addAndGet(pointsInBatch); // 현재 누적 개수

        // --- 추가된 로그: 매번 배치 저장 후 누적 개수 출력 ---
        log.info("저장 성공: 배치 {}개 / 총 {}개 저장 완료.", pointsInBatch, currentTotal);
        // --- ---

        // --- 기존 마일스톤 로그 로직 ---
        long previousMilestone = loggedMilestoneCounter.get();
        long currentMilestoneTarget = (previousMilestone + 1) * POINTS_PER_GENERATION;

        if (currentTotal >= currentMilestoneTarget && previousTotal < currentMilestoneTarget) {
             long achievedMilestone = currentTotal / POINTS_PER_GENERATION;
             loggedMilestoneCounter.set(achievedMilestone);
             // 마일스톤 로그 메시지 약간 수정 (가독성)
             log.info(">>>>>>>>>> 마일스톤 달성! 약 {} * {} = {} 포인트 저장 돌파 (현재 누적: {}) <<<<<<<<<<",
                      achievedMilestone, POINTS_PER_GENERATION, achievedMilestone * POINTS_PER_GENERATION, currentTotal);
        }
        // --- ---
    }
}

/* === 이전 saveBatch(List<Point>, long) 메서드는 제거 또는 주석 처리 ===
private void saveBatch(List<Point> pointsToSave, long attemptCount) { ... }
*/

/* === 이전 방식 주석 처리 또는 제거 ===
// public void runOnceOnStartup() { ... }
// public void generateAndSaveInBatches(long attemptCount) { ... }
// public void generateAndSaveAllAtOnce(long attemptCount) { ... }
*/
}
