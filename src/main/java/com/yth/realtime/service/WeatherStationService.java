package com.yth.realtime.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.yth.realtime.entity.WeatherStation;
import com.yth.realtime.repository.WeatherStationRepository;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
// @RequiredArgsConstructor

@Slf4j
@Service
@Transactional
public class WeatherStationService {
    private final Dotenv dotenv;
    private final String apiKey;
    private final WeatherStationRepository weatherStationRepository;
    private final RestTemplate restTemplate;
    
 @Autowired
    public WeatherStationService(WeatherStationRepository weatherStationRepository) {
        this.dotenv = Dotenv.load();
        this.apiKey = dotenv.get("WEATHER_API_KEY");
        this.weatherStationRepository = weatherStationRepository;
        this.restTemplate = new RestTemplate();
    }


    public List<WeatherStation> getAllStations() {
        try {
            // 캐시된 데이터가 있는지 확인
            List<WeatherStation> cachedStations = weatherStationRepository.findAll();
            if (!cachedStations.isEmpty()) {
                return cachedStations;
            }

 
            // API 호출
            String apiUrl = String.format(
                "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=SFC&stn=&tm=202211300900&help=0&authKey=%s",
                apiKey
            );
            log.info("누락 데이터 API 호출: {}", apiUrl);

            
            String response = restTemplate.getForObject(apiUrl, String.class);
            List<WeatherStation> stations = parseStationData(response);
            log.info("저장할 데이터 개수: {}", stations.size());
            // DB에 저장
            weatherStationRepository.saveAll(stations);
            log.info("데이터 저장 완료");
            return stations;
    
            
        } catch (Exception e) {
            log.error("관측소 정보 조회 실패: {}", e.getMessage());
            throw new RuntimeException("관측소 정보를 가져오는데 실패했습니다.", e);
        }
    }

    private List<WeatherStation> parseStationData(String data) {
        List<WeatherStation> stations = new ArrayList<>();
        
        try {

            

            List<String> validLines = new ArrayList<>();
            String[] lines = data.split("\n");

            for (String line : lines) {
                if (line.trim().equals("#7777END")) break;
                if (!line.trim().isEmpty()) {
                    validLines.add(line);
                }
            }
            // 헤더 찾기 (STN 포함된 라인)
            int headerIndex = -1;
            for (int i = 0; i < validLines.size(); i++) {
                if (validLines.get(i).contains("STN")) {
                    headerIndex = i;
                    break;
                }
            }
            
            if (headerIndex == -1) {
                log.error("헤더를 찾을 수 없습니다");
                return Collections.emptyList();
            }

            String[] headers = validLines.get(headerIndex).replace("#", "").trim().split("\\s+");
            log.info("파싱된 헤더: {}", Arrays.toString(headers));
                
 // 데이터 파싱
            for (int i = headerIndex + 1; i < validLines.size(); i++) {
                String line = validLines.get(i).trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                String[] values = line.split("\\s+");
                
                // 데이터 검증
                if (values.length < 3) {  // 최소 필요한 컬럼 수 확인
                    log.warn("라인 {} 스킵: 데이터가 부족함", i);
                    continue;
                }

                WeatherStation station = new WeatherStation();
                station.setStnId(values[0]); // 지점번호
                
                // 기온(TA)과 풍속(WS) 데이터 위치 찾기
                int stnKoIndex  = -1;
                int lonIndex = -1;
                int latIndex = -1;
                for (int j = 0; j < headers.length; j++) {
                    if (headers[j].equals("STN_KO")) stnKoIndex = j;
                    if (headers[j].equals("LON")) lonIndex = j;
                    if (headers[j].equals("LAT")) latIndex = j;
                }

                // 찾은 인덱스로 데이터 추출
            if (lonIndex != -1 && lonIndex < values.length) {
                station.setLongitude(Double.parseDouble(values[lonIndex]));
            }
            if (latIndex != -1 && latIndex < values.length) {
                station.setLatitude(Double.parseDouble(values[latIndex]));
            }
            if (stnKoIndex != -1 && stnKoIndex < values.length) {
                station.setStnName(values[stnKoIndex]);
            }
            stations.add(station); 

               
                log.debug("파싱된 데이터: {}", station);  // 각 데이터 로깅
            }

            return stations;
            
        } catch (Exception e) {
            log.error("관측소 데이터 파싱 실패: {}", e.getMessage());
            throw new RuntimeException("관측소 데이터 파싱에 실패했습니다.", e);
        }
    }

    public WeatherStation findNearestStation(double lat, double lon) {
        List<WeatherStation> stations = getAllStations();
        WeatherStation nearest = null;
        double minDistance = Double.MAX_VALUE;

        log.info("입력 위치 - 위도: {}, 경도: {}", lat, lon);
        
        for (WeatherStation station : stations) {
            double distance = calculateDistance(lat, lon, 
                station.getLatitude(), station.getLongitude());
            
            if (distance < minDistance) {
                minDistance = distance;
                nearest = station;
                log.debug("관측소 후보 - 지점: {}, 거리: {}km", 
                    station.getStnName(), distance);
            }
        }

        if (nearest != null) {
            nearest.setDistance(minDistance);
            log.info("선택된 관측소 - 지점: {}, 거리: {}km", 
                nearest.getStnName(), minDistance);
        } else {
            log.warn("관측소를 찾을 수 없습니다.");
        }

        return nearest;
    }

    // Haversine formula를 사용하여 두 지점 간의 거리 계산 (km)
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // 지구의 반지름 (km)
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
} 