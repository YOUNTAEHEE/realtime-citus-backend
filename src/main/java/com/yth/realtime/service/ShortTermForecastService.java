package com.yth.realtime.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.yth.realtime.repository.WeatherRepository;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ShortTermForecastService {

    private final Dotenv dotenv;

    private final String apiKey;

    private final WeatherRepository weatherRepository;

    private final RestTemplate restTemplate;

    @Autowired
    public ShortTermForecastService(WeatherRepository weatherRepository) {
        this.dotenv = Dotenv.load();
        this.apiKey = dotenv.get("WEATHER_API_KEY");
        this.weatherRepository = weatherRepository;
        this.restTemplate = new RestTemplate();
    }

    public List<Map<String, String>> fetchShortTermForecast(String region) {
        LocalDate today = LocalDate.now();
        LocalDate todayLater = today.plusDays(7);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedToday = today.format(formatter);
        String formattedTodayLater = todayLater.format(formatter);

        log.info("단기 예보 기간: {} ~ {}", formattedToday, formattedTodayLater);

        // API 호출
        String apiUrl = String.format(
                "https://apihub.kma.go.kr/api/typ01/url/fct_afs_dl.php?stn=%s&tmfc1=%s&tmfc2=%s&disp=0&help=0&authKey=%s",
                region, formattedToday, formattedTodayLater, apiKey);
        log.info("누락 데이터 API 호출: {}", apiUrl);
        System.out.println("region: " + region);

        try {
            String response = restTemplate.getForObject(apiUrl, String.class);
            List<Map<String, String>> parsedData = parseWeatherData(response, region);

            return parsedData;
        } catch (Exception e) {
            log.error("날짜 {} 데이터 조회 실패: {}", today, e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<Map<String, String>> parseWeatherData(String data, String requestedStn) {
        if (data == null || data.trim().isEmpty()) {
            log.error("받은 데이터가 비어있습니다");
            return Collections.emptyList();
        }

        try {
            List<Map<String, String>> result = new ArrayList<>();
            String[] lines = data.split("\n");
            // log.info("원본 데이터: {}", data);

            // #7777END 이전의 유효한 데이터만 처리
            List<String> validLines = new ArrayList<>();
            for (String line : lines) {
                if (line.trim().equals("#7777END"))
                    break;
                if (!line.trim().isEmpty()) {
                    validLines.add(line);
                }
            }

            // 헤더 찾기 (YYMMDDHHMI가 포함된 라인)
            int headerIndex = -1;
            for (int i = 0; i < validLines.size(); i++) {
                if (validLines.get(i).contains("REG_ID")) {
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
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String[] values = line.split("\\s+");

                // 데이터 검증
                if (values.length < 3) { // 최소 필요한 컬럼 수 확인
                    log.warn("라인 {} 스킵: 데이터가 부족함", i);
                    continue;
                }

                Map<String, String> entry = new HashMap<>();
                entry.put("REG_ID", values[0]); // 시간

                // 기온(TA)과 풍속(WS) 데이터 위치 찾기
                int stnIndex = -1;
                int taIndex = -1;
                int stIndex = -1;
                int wfIndex = -1;
                int tm_fcIndex = -1;
                int tm_efIndex = -1;
                for (int j = 0; j < headers.length; j++) {
                    if (headers[j].equals("STN"))
                        stnIndex = j;
                    if (headers[j].equals("TA"))
                        taIndex = j;
                    if (headers[j].equals("ST"))
                        stIndex = j;
                    if (headers[j].equals("WF"))
                        wfIndex = j;
                    if (headers[j].equals("TM_FC"))
                        tm_fcIndex = j;
                    if (headers[j].equals("TM_EF"))
                        tm_efIndex = j;
                }

                // 찾은 인덱스로 데이터 추출
                if (stnIndex != -1 && stnIndex < values.length && 
                values[stnIndex].equals(requestedStn)) {
                    entry.put("STN", values[stnIndex]);
                }
                if (taIndex != -1 && taIndex < values.length) {
                    entry.put("TA", values[taIndex]);
                }
                if (stIndex != -1 && stIndex < values.length) {
                    entry.put("ST", values[stIndex]);
                }
                if (wfIndex != -1 && wfIndex < values.length) {
                    entry.put("WF", values[wfIndex]);
                }
                if (tm_fcIndex != -1 && tm_fcIndex < values.length) {
                    entry.put("TM_FC", values[tm_fcIndex]);
                }
                if (tm_efIndex != -1 && tm_efIndex < values.length) {
                    entry.put("TM_EF", values[tm_efIndex]);
                }

                result.add(entry);
                log.debug("파싱된 데이터: {}", entry); // 각 데이터 로깅
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

}
