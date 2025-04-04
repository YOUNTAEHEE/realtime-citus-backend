package com.yth.realtime.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Service
@Transactional
@Slf4j
@EnableScheduling
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

    public ModbusService(InfluxDBService influxDBService,
            ModbusDeviceRepository modbusDeviceRepository,
            SettingsRepository settingsRepository,
            ApplicationEventPublisher eventPublisher) {
        this.influxDBService = influxDBService;
        this.settingsRepository = settingsRepository;
        this.modbusDeviceRepository = modbusDeviceRepository;
        this.eventPublisher = eventPublisher;
    }

    // @PostConstruct
    // public void init() {
    // log.info("ModbusService 초기화 시작");
    // try {
    // // 여기에 기존 초기화 코드

    // log.info("ModbusService 초기화 완료");
    // } catch (Exception e) {
    // log.error("ModbusService 초기화 실패: {}", e.getMessage(), e);
    // }
    // }

    // 테스트 코드 추가
    // @PostConstruct
    // public void runCsvTest() {
    // long currentCount = csvInsertCounter.incrementAndGet();
    // startCsvBatchInsertAndQueue(currentCount);
    // }
    @Scheduled(fixedRate = 1000) // 1초마다 실행
    public void repeatCsvInsert() {
        long currentCount = csvInsertCounter.incrementAndGet();
        log.info("CSV 데이터 삽입 시도 #{}", currentCount);
        startCsvBatchInsertAndQueue(currentCount);

    }

    public void startCsvBatchInsertAndQueue(long attemptCount) {
        String csvFilePath = "C:/Users/CITUS/Desktop/modbusdata/all_racks_combined.csv";
        long startTime = System.currentTimeMillis();

        // 나눠서 코드
        long processStartTime = System.currentTimeMillis();
        int totalLinesRead = 0;
        int totalPointsSaved = 0;
        final int BATCH_SIZE = 5016; // 배치 크기
        List<Point> currentBatch = new ArrayList<>(BATCH_SIZE);
        // 나눠서코드끝
        AtomicInteger totalPointsQueued = new AtomicInteger(0);

        // log.info("CSV 파일 로딩 및 데이터 큐잉 시작: {}", csvFilePath);

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                totalPointsQueued.incrementAndGet();
                String trimmedLine = line.trim();

                // === 추가: 헤더 행 건너뛰기 ===
                // CSV 파일의 실제 헤더 내용과 정확히 일치하는지 확인 필요
                if (trimmedLine.startsWith("rack_id,module_id,cell_id")) {
                    log.debug("CSV 헤더 행 건너뜁니다: {}", trimmedLine);
                    continue; // 다음 줄 처리로 넘어감
                }
                // === 추가 끝 ===

                try {
                    // 원본 행과 클린된 행 로깅 (디버깅용)
                    String cleanedLine = trimmedLine.replaceAll("^,+", "");
                    // log.debug("원본 행: [{}], 클린된 행: [{}]", trimmedLine, cleanedLine);
                    String[] row = cleanedLine.split(",");

                    // CSV 파일의 구조에 맞게 조건을 확인
                    if (row.length >= 15) { // 최소 컬럼 수를 15개로 변경 (ID 3개 + 값 6개 + 디바이스 ID 6개)
                        // === 추가 시작 ===
                        String rackId = row[0].trim();
                        String moduleId = row[1].trim();
                        String cellId = row[2].trim();
                        // === 추가 끝 ===

                        // 인덱스 조정 (기존 인덱스 + 3)
                        double temperatureMin = Double.parseDouble(row[3].trim()); // 0 -> 3
                        double temperatureMax = Double.parseDouble(row[4].trim()); // 1 -> 4
                        double temperatureAvg = Double.parseDouble(row[5].trim()); // 2 -> 5
                        double humidityMin = Double.parseDouble(row[6].trim()); // 3 -> 6
                        double humidityMax = Double.parseDouble(row[7].trim()); // 4 -> 7
                        double humidityAvg = Double.parseDouble(row[8].trim()); // 5 -> 8
                        String temperatureMinDevice = row[9].trim(); // 6 -> 9
                        String temperatureMaxDevice = row[10].trim(); // 7 -> 10
                        String temperatureAvgDevice = row[11].trim(); // 8 -> 11
                        String humidityMinDevice = row[12].trim(); // 9 -> 12
                        String humidityMaxDevice = row[13].trim(); // 10 -> 13
                        String humidityAvgDevice = row[14].trim(); // 11 -> 14

                        String deviceId = "CsvTestDevice";
                        String host = "CsvTestHost";
                        String measurement = "sensor_data_csv_test7"; // 필요 시 measurement 이름 변경 고려

                        Point point = Point.measurement(measurement)
                                // === 추가 시작 ===
                                .addTag("rack_id", rackId)
                                .addTag("module_id", moduleId)
                                .addTag("cell_id", cellId)
                                // === 추가 끝 ===
                                .addField("temperature_min", temperatureMin)
                                .addField("temperature_max", temperatureMax)
                                .addField("temperature_avg", temperatureAvg)
                                .addField("humidity_min", humidityMin)
                                .addField("humidity_max", humidityMax)
                                .addField("humidity_avg", humidityAvg)
                                .addTag("temperature_min_device", temperatureMinDevice)
                                .addTag("temperature_max_device", temperatureMaxDevice)
                                .addTag("temperature_avg_device", temperatureAvgDevice)
                                .addTag("humidity_min_device", humidityMinDevice)
                                .addTag("humidity_max_device", humidityMaxDevice)
                                .addTag("humidity_avg_device", humidityAvgDevice)
                                .time(Instant.now(), WritePrecision.MS);

                        // pointQueue.put(point);//한번에 코드
                        // 배치 리스트에 추가
                        currentBatch.add(point);// 나눠서 코드
                        // 배치가 꽉 차면 저장
                        if (currentBatch.size() >= BATCH_SIZE) {
                            log.debug("배치 크기 ({}) 도달, DB 저장 시작 (시도 #{})", BATCH_SIZE, attemptCount);
                            try {
                                influxDBService.savePoints(new ArrayList<>(currentBatch)); // 방어적 복사본 전달
                                totalPointsSaved += currentBatch.size();
                            } catch (Exception e) {
                                // 배치 저장 실패 시 로그 강화
                                log.error("!!! InfluxDB 배치 저장 실패 (시도 #{}, 배치 크기 {}). 데이터 유실 가능성 !!!",
                                        attemptCount, currentBatch.size(), e);
                            } finally {
                                currentBatch.clear(); // 성공/실패 여부와 관계없이 배치는 비움 (유실 감수)
                            }
                        } // 나눠서코드
                    } else {
                        log.warn("CSV 행 열 개수 부족 ({}개) (행 #{}, 내용: {})", row.length, totalLinesRead, trimmedLine);
                    }
                } catch (NumberFormatException e) {
                    log.warn("숫자 변환 실패 (행 #{}, 내용: {}): {}", totalLinesRead, trimmedLine, e.getMessage());
                } catch (ArrayIndexOutOfBoundsException e) {
                    log.warn("CSV 인덱스 오류 (행 #{}, 내용: {}): {}", totalLinesRead, trimmedLine, e.getMessage());
                } catch (Exception e) { // 다른 예외 처리
                    log.error("CSV 행 처리 중 오류 (행 #{}, 내용: {}): {}", totalLinesRead, trimmedLine, e.getMessage(), e);
                }
            } // end while

            // 루프 종료 후 남은 배치 저장
            if (!currentBatch.isEmpty()) {
                log.debug("마지막 남은 배치 ({}) DB 저장 시작 (시도 #{})", currentBatch.size(), attemptCount);
                try {
                    influxDBService.savePoints(new ArrayList<>(currentBatch)); // 방어적 복사본 전달
                    totalPointsSaved += currentBatch.size();
                } catch (Exception e) {
                    log.error("!!! InfluxDB 마지막 배치 저장 실패 (시도 #{}, 배치 크기 {}). 데이터 유실 가능성 !!!",
                            attemptCount, currentBatch.size(), e);
                } finally {
                    currentBatch.clear();
                }
            }

        } catch (IOException e) {
            log.error("CSV 파일 읽기 실패 ({}): {}", csvFilePath, e.getMessage(), e);
            return; // 파일 읽기 실패 시 현재 시도 중단
        } catch (Exception e) {
            log.error("CSV 처리 중 예기치 않은 오류 발생 (시도 #{})", attemptCount, e);
        } finally {
            long processEndTime = System.currentTimeMillis();
            long duration = processEndTime - processStartTime;
            log.info("CSV 파일 처리 완료 (시도 #{}) - 총 읽은 줄: {}, 저장된 포인트: {}. 소요 시간: {} ms",
                    attemptCount, totalLinesRead, totalPointsSaved, duration);
            // === 1초 목표 달성 여부 확인 ===
            if (duration <= 1000) {
                log.info(">>>> 성공: CSV 처리 및 저장 작업이 1초 안에 완료되었습니다! (시도 #{})", attemptCount);
            } else {
                log.warn("<<<< 경고: CSV 처리 및 저장 작업이 1초를 초과했습니다. ({} ms 소요, 시도 #{})", duration, attemptCount);
            }
        }
    }

    // 큐의 모든 데이터를 한 번에 DB에 저장 시도하는 메서드
    // private void processEntireQueue() {
    // List<Point> batchToSave = new ArrayList<>();
    // pointQueue.drainTo(batchToSave); // 큐의 모든 요소 가져오기

    // if (!batchToSave.isEmpty()) {
    // log.warn("!!! 경고: 큐의 모든 데이터 {}개를 한 번의 배치로 저장 시도합니다. (매우 위험!) !!!",
    // batchToSave.size());
    // try {
    // influxDBService.savePoints(batchToSave);
    // // 성공 로그는 InfluxDBService에서 출력될 것임
    // } catch (Exception e) {
    // log.error("!!! 전체 큐 데이터 처리 중 InfluxDB 저장 실패. {}개의 포인트 유실 가능성. !!!",
    // batchToSave.size(), e);
    // // 여기에서 실패 시 어떻게 할지 결정해야 함 (예: 로그만 남기기, 재시도 불가)
    // }
    // } else {
    // log.info("처리할 큐 데이터가 없습니다.");
    // }
    // }

    // 기존 processQueue 메서드는 이제 사용되지 않으므로 주석 처리 또는 삭제
    /*
     * @Scheduled(fixedDelay = 10) // 이전 버전
     * public void processQueue() { ... }
     */

    // 애플리케이션 종료 시 스레드 풀 및 남은 큐 데이터 처리
    @PreDestroy
    public void cleanupQueueOnShutdown() {
        log.info("애플리케이션 종료 전 남은 큐 데이터 확인...");
        if (!pointQueue.isEmpty()) {
            // 애플리케이션 종료 시점에는 이미 processEntireQueue가 호출되었을 가능성이 높음
            // 하지만 만약의 경우를 대비해 로그만 남기거나, 간단한 처리 시도
            log.warn("애플리케이션 종료 시점에 큐에 아직 {}개의 포인트가 남아있습니다. (처리 시도 안 함)", pointQueue.size());
            // List<Point> remainingPoints = new ArrayList<>();
            // pointQueue.drainTo(remainingPoints);
            // try { influxDBService.savePoints(remainingPoints); } catch (Exception e) {
            // ... }
        } else {
            log.info("종료 시점에 큐가 비어있습니다.");
        }
        // 기존 다른 cleanup 로직 호출 (예: Modbus 연결 해제 등)
        // cleanupModbusConnections(); // 별도 메서드라고 가정
    }

    // Modbus 연결 해제 등 다른 cleanup 로직을 위한 메서드 (예시)
    private void cleanupModbusConnections() {
        log.info("Modbus 연결 정리 시작...");
        modbusMasters.forEach((deviceId, master) -> {
            try {
                if (master != null && master.isConnected()) { // 연결 상태 확인 추가
                    master.disconnect();
                    log.info("Modbus 연결 해제 성공: {}", deviceId);
                }
            } catch (Exception e) {
                log.error("Modbus 연결 해제 실패: {}", deviceId, e);
            }
        });
        modbusMasters.clear();
        lastDataSaveTime.clear(); // 관련 있다면 이것도 정리
        registeredDevices.clear(); // 관련 있다면 이것도 정리
        log.info("Modbus 연결 정리 완료.");
    }
    // 테스트 코드 끝

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

    /**
     * Modbus 데이터를 읽어서 InfluxDB에 저장하고 저장된 데이터를 조회하여 반환
     */
    // public Map<String, Object> readModbusData(ModbusDevice device) throws
    // Exception {
    // ModbusTCPMaster master = getOrCreateConnection(device);
    // try {
    // // 1. 장치에서 레지스터 값 읽기
    // Register[] registers = master.readMultipleRegisters(
    // device.getSlaveId(),
    // device.getStartAddress(),
    // device.getLength());

    // // 2. 온도와 습도 값 스케일링
    // double temperature = registers[0].getValue() / 10.0;
    // double humidity = registers[1].getValue() / 10.0;

    // // 3. InfluxDB에 데이터 저장
    // long currentTime = System.currentTimeMillis() / 1000;
    // Long lastSave = lastDataSaveTime.get(device.getDeviceId());

    // if (lastSave == null || (currentTime - lastSave) >= SAVE_INTERVAL) {
    // try {
    // influxDBService.saveSensorData(temperature, humidity, device.getHost(),
    // device.getDeviceId());
    // lastDataSaveTime.put(device.getDeviceId(), currentTime);
    // log.debug("데이터 저장 완료 - deviceId: {}, temp: {}, humidity: {}",
    // device.getDeviceId(), temperature, humidity);
    // } catch (Exception e) {
    // log.error("데이터 저장 실패: {} - {}", device.getDeviceId(), e.getMessage());
    // }
    // }

    // // 4. 저장된 최신 데이터 조회
    // // Map<String, Object> latestData =
    // influxDBService.getLatestSensorData(device.getDeviceId());

    // // 5. 조회 결과가 없으면 직접 읽은 데이터 사용
    // // if (latestData == null || latestData.isEmpty()) {
    // // latestData = new HashMap<>();
    // // latestData.put("temperature", temperature);
    // // latestData.put("humidity", humidity);
    // // latestData.put("deviceId", device.getDeviceId());
    // // latestData.put("time", LocalDateTime.now());
    // // }

    // log.debug("반환 데이터: {}", latestData);
    // return latestData;
    // } catch (Exception e) {
    // log.error("데이터 읽기 실패: {} - {}", device.getDeviceId(), e.getMessage());
    // throw e;
    // }
    // }

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
