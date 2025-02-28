package com.yth.realtime.service;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.entity.ModbusDeviceDocument;
import com.yth.realtime.entity.Weather;
import com.yth.realtime.repository.WeatherRepository;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Transactional
public class WeatherService {

    private final Dotenv dotenv;

    private final String apiKey;

    private final WeatherRepository weatherRepository;

    private final RestTemplate restTemplate;

    @Autowired
    public WeatherService(WeatherRepository weatherRepository) {
        this.dotenv = Dotenv.load();
        this.apiKey = dotenv.get("WEATHER_API_KEY");
        this.weatherRepository = weatherRepository;
        this.restTemplate = new RestTemplate();
    }

    public List<Map<String, String>> fetchWeatherData(String dateFirst, String dateLast, String region) {
        // log.info("=== 날씨 데이터 조회 시작 ===");
        // log.info("요청 파라미터: dateFirst={}, dateLast={}, region={}", dateFirst, dateLast, region);
        
        // 날짜 형식 변환 및 로깅 추가
        String startDateTime = dateFirst.replaceAll("-", "") + "0000";
        String endDateTime = dateLast.replaceAll("-", "") + "2359";
        log.info("조회 기간: {} ~ {}", startDateTime, endDateTime);
        
        // MongoDB에서 데이터 조회
        List<Weather> savedData = weatherRepository.findByRegionAndDateTimeBetweenOrderByDateTimeDesc(
            region, startDateTime, endDateTime);
        log.info("조회된 데이터 수: {}", savedData.size());
        
        // 데이터 유효성 검사 추가
        if (savedData.isEmpty()) {
            log.warn("해당 기간의 데이터가 없습니다.");
        } else {
            log.info("첫 번째 데이터: {}, 마지막 데이터: {}", 
                savedData.get(0).getDateTime(), 
                savedData.get(savedData.size()-1).getDateTime());
        }
        
        // 요청한 날짜 범위의 모든 날짜
        List<String> requestedDates = getDateRange(dateFirst, dateLast);
        
        // 저장된 데이터의 날짜들
        Set<String> savedDates = savedData.stream()
            .map(w -> w.getDateTime().substring(0, 8))
            .collect(Collectors.toSet());
        
        // 누락된 날짜 확인
        List<String> missingDates = requestedDates.stream()
            .filter(date -> !savedDates.contains(date))
            .collect(Collectors.toList());
        
        if (!missingDates.isEmpty()) {
            log.info("누락된 날짜 발견: {}", missingDates);
            
            // 누락된 각 날짜에 대해 API 호출
            for (String date : missingDates) {
                String apiStartTime = date + "0000";
                String apiEndTime = date + "2359";
                
                // API 호출
                String apiUrl = String.format(
                    "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php?tm1=%s&tm2=%s&stn=%s&help=0&authKey=%s",
                    apiStartTime, apiEndTime, region, apiKey
                );
                log.info("누락 데이터 API 호출: {}", apiUrl);
                
                try {
                    String response = restTemplate.getForObject(apiUrl, String.class);
                    List<Map<String, String>> parsedData = parseWeatherData(response);
                    
                    if (!parsedData.isEmpty()) {
                        List<Weather> weatherEntities = parsedData.stream()
                            .map(data -> Weather.fromMap(data, region))
                            .collect(Collectors.toList());
                        
                        weatherRepository.saveAll(weatherEntities);
                        log.info("날짜 {} 데이터 저장 완료: {} 건", date, weatherEntities.size());
                        
                        // 저장된 데이터를 savedData에 추가
                        savedData.addAll(weatherEntities);
                    }
                } catch (Exception e) {
                    log.error("날짜 {} 데이터 조회 실패: {}", date, e.getMessage());
                }
                
                // API 호출 간격 조절 (필요한 경우)
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        // 전체 데이터를 날짜 기준 내림차순 정렬
        List<Weather> sortedData = savedData.stream()
            .sorted((a, b) -> b.getDateTime().compareTo(a.getDateTime()))
            .collect(Collectors.toList());
        
        List<Map<String, String>> result = sortedData.stream()
            .map(Weather::toMap)
            .collect(Collectors.toList());
        
        return result;
    }

    private List<String> getDateRange(String startDate, String endDate) {
        List<String> dates = new ArrayList<>();
        LocalDate start = LocalDate.parse(startDate, DateTimeFormatter.BASIC_ISO_DATE);
        LocalDate end = LocalDate.parse(endDate, DateTimeFormatter.BASIC_ISO_DATE);
        
        while (!start.isAfter(end)) {
            dates.add(start.format(DateTimeFormatter.BASIC_ISO_DATE));
            start = start.plusDays(1);
        }
        return dates;
    }

    private List<Map<String, String>> parseWeatherData(String data) {
        if (data == null || data.trim().isEmpty()) {
            log.error("받은 데이터가 비어있습니다");
            return Collections.emptyList();
        }

        try {
            List<Map<String, String>> result = new ArrayList<>();
            String[] lines = data.split("\n");
            log.info("원본 데이터: {}", data);
            
            // #7777END 이전의 유효한 데이터만 처리
            List<String> validLines = new ArrayList<>();
            for (String line : lines) {
                if (line.trim().equals("#7777END")) break;
                if (!line.trim().isEmpty()) {
                    validLines.add(line);
                }
            }
            
            // 헤더 찾기 (YYMMDDHHMI가 포함된 라인)
            int headerIndex = -1;
            for (int i = 0; i < validLines.size(); i++) {
                if (validLines.get(i).contains("YYMMDDHHMI")) {
                    headerIndex = i;
                    break;
                }
            }
            
            if (headerIndex == -1) {
                log.error("헤더를 찾을 수 없습니다");
                return Collections.emptyList();
            }

            // 헤더 파싱
            String[] headers = validLines.get(headerIndex).replace("#", "").trim().split("\\s+");
            log.info("파싱된 헤더: {}", Arrays.toString(headers));

            // 데이터 파싱
            for (int i = headerIndex + 1; i < validLines.size(); i++) {
                String line = validLines.get(i).trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                String[] values = line.split("\\s+");
                
                // 데이터 검증
                if (values.length < 4) {  // 최소 필요한 컬럼 수 확인
                    log.warn("라인 {} 스킵: 데이터가 부족함", i);
                    continue;
                }

                Map<String, String> entry = new HashMap<>();
                entry.put("YYMMDDHHMI", values[0]);  // 시간
                
                // 기온(TA)과 풍속(WS) 데이터 위치 찾기
                int taIndex = -1;
                int wsIndex = -1;
                int hmIndex = -1;
                int wdIndex = -1;
                for (int j = 0; j < headers.length; j++) {
                    if (headers[j].equals("TA")) taIndex = j;
                    if (headers[j].equals("WS")) wsIndex = j;
                    if (headers[j].equals("HM")) hmIndex = j;
                    if (headers[j].equals("WD")) wdIndex = j;
                }

                // 찾은 인덱스로 데이터 추출
                if (taIndex != -1 && taIndex < values.length) {
                    entry.put("TA", values[taIndex]);
                }
                if (wsIndex != -1 && wsIndex < values.length) {
                    entry.put("WS", values[wsIndex]);
                }
                if (hmIndex != -1 && hmIndex < values.length) {
                    entry.put("HM", values[hmIndex]);
                }
                if (wdIndex != -1 && wdIndex < values.length) {
                    entry.put("WD", values[wdIndex]);
                }

                result.add(entry);
                log.debug("파싱된 데이터: {}", entry);  // 각 데이터 로깅
            }

            log.info("파싱된 데이터 개수: {}", result.size());
            if (!result.isEmpty()) {
                log.info("첫 번째 데이터: {}", result.get(0));
            }
            return result;

        } catch (Exception e) {
            log.error("데이터 파싱 중 오류 발생: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    public Map<String, Object> findTemperatureExtreme(String date, String type, String region) {
        try {
            log.info("=== 최고/최저 온도 검색 시작 ===");
            log.info("입력 파라미터 - 날짜: {}, 타입: {}, 지역: {}", date, type, region);
            
            String startDateTime = date.replaceAll("-", "").trim() + "0000";
            String endDateTime = date.replaceAll("-", "").trim() + "2359";
            
            log.info("변환된 날짜시간 - 시작: {}, 종료: {}", startDateTime, endDateTime);
            
            List<Weather> periodData = weatherRepository.findByRegionAndDateTimeBetweenOrderByDateTimeDesc(
                region, startDateTime, endDateTime);
            
            log.info("조회된 전체 데이터 수: {}", periodData.size());
            
            // 온도 데이터 필터링 과정 로깅
            List<Weather> validTempData = periodData.stream()
                .filter(w -> w.getTemperature() != null && !w.getTemperature().trim().isEmpty())
                .filter(w -> {
                    try {
                        Double.parseDouble(w.getTemperature());
                        return true;
                    } catch (NumberFormatException e) {
                        log.warn("유효하지 않은 온도 값: {}", w.getTemperature());
                        return false;
                    }
                })
                .collect(Collectors.toList());
            
            log.info("유효한 온도 데이터 수: {}", validTempData.size());
            
            if (validTempData.isEmpty()) {
                log.warn("유효한 온도 데이터가 없습니다");
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("success", false);
                errorResult.put("message", "해당 날짜의 유효한 기온 데이터가 없습니다");
                return errorResult;
            }

            // 최고/최저 온도 찾기
            Weather extremeTemp = validTempData.stream()
                .min((a, b) -> {
                    double tempA = Double.parseDouble(a.getTemperature());
                    double tempB = Double.parseDouble(b.getTemperature());
                    int comparison = type.equals("high_temp") 
                        ? Double.compare(tempB, tempA)
                        : Double.compare(tempA, tempB);
                    log.debug("온도 비교: {} vs {} = {}", tempA, tempB, comparison);
                    return comparison;
                })
                .orElse(null);

            if (extremeTemp != null) {
                log.info("찾은 {} 온도: {}°C, 시간: {}", 
                    type.equals("high_temp") ? "최고" : "최저",
                    extremeTemp.getTemperature(),
                    extremeTemp.getDateTime());
                
                String datetime = extremeTemp.getDateTime();
                String formattedDate = datetime.substring(0, 4) + "-" + 
                                     datetime.substring(4, 6) + "-" + 
                                     datetime.substring(6, 8);
                String formattedTime = datetime.substring(8, 10) + ":" + 
                                     datetime.substring(10, 12);
                
                Map<String, Object> result = new HashMap<>();
                result.put("success", true);
                result.put("datetime", formattedDate + " " + formattedTime);
                result.put("temperature", extremeTemp.getTemperature());
                result.put("windSpeed", extremeTemp.getWindSpeed());
                result.put("humidity", extremeTemp.getHumidity());
                result.put("windDirection", extremeTemp.getWindDirection());
                
                log.info("검색 결과: {}", result);
                return result;
            }
            
            return null;
            
        } catch (Exception e) {
            log.error("온도 검색 중 오류 발생: {}", e.getMessage(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("success", false);
            errorResult.put("message", "온도 검색 중 오류가 발생했습니다");
            errorResult.put("error", e.getMessage());
            return errorResult;
        }
    }



 

}