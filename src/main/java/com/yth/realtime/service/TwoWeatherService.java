package com.yth.realtime.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import com.yth.realtime.entity.Weather;

@Service
public class TwoWeatherService {

        private static final Logger log = LoggerFactory.getLogger(TwoWeatherService.class);

        @Autowired
        private MongoTemplate mongoTemplate;

        public TwoWeatherService(MongoTemplate mongoTemplate) {
                this.mongoTemplate = mongoTemplate;
        }

        public Map<String, Double> getTemperatureDifference(
                        String regionCode1, String regionCode2,
                        long startTime1, long endTime1,
                        long startTime2, long endTime2) {

                String startTime01 = String.valueOf(startTime1) + "0000";
                String endTime01 = String.valueOf(endTime1) + "2359";

                String startTime02 = String.valueOf(startTime2) + "0000";
                String endTime02 = String.valueOf(endTime2) + "2359";

                // regionCode1에 대한 평균 기온 계산
                double avgTemp1 = calculateAverageTemperature(regionCode1, startTime01, endTime01);

                // regionCode2에 대한 평균 기온 계산
                double avgTemp2 = calculateAverageTemperature(regionCode2, startTime02, endTime02);

                // 두 평균 기온의 차이 반환 (절대값)
                double temperatureDifference = Math.abs(avgTemp1 - avgTemp2);

                Map<String, Double> map = new HashMap<>();
                map.put("region1Average", Math.round(avgTemp1 * 100.0) / 100.0);
                map.put("region2Average", Math.round(avgTemp2 * 100.0) / 100.0);
                map.put("temperatureDifference", Math.round(temperatureDifference * 100.0) / 100.0);

                return map;
        }

        private double calculateAverageTemperature(String region, String startTime, String endTime) {
                log.info("calculateAverageTemperature 메서드 시작: region={}, startTime={}, endTime={}",
                                region, startTime, endTime);

                try {

                        log.info("날짜 검색 범위: {} ~ {}, 지역: {}", startTime, endTime, region);

                        // 먼저 해당 지역의 데이터가 있는지 확인
                        Query regionQuery = new Query(Criteria.where("region").is(region));
                        long regionCount = mongoTemplate.count(regionQuery, "weather");
                        log.info("지역 '{}'의 데이터 총 {}개 있음", region, regionCount);

                        if (regionCount == 0) {
                                log.warn("지역 '{}'에 대한 데이터가 없습니다.", region);
                                return 0.0;
                        }

                        // dateTime 필드에 대한 집계 쿼리를 위한 Criteria 구성
                        // MongoDB Collection에 저장된 실제 데이터 형식에 맞춤
                        Criteria criteria = Criteria.where("region").is(region)
                                        .and("dateTime").gte(startTime).lte(endTime);

                        // 집계 쿼리 실행
                        Aggregation aggregation = Aggregation.newAggregation(
                                        Aggregation.match(criteria),
                                        Aggregation.project()
                                                        .andExpression("{ $toDouble: '$temperature' }").as("tempNum"), // 문자열을
                                                                                                                       // double로
                                                                                                                       // 변환
                                        Aggregation.group().avg("tempNum").as("avgTemperature")); // 변환된 값으로 평균 계산

                        log.info("MongoDB 집계 쿼리: {}", aggregation.toString());

                        AggregationResults<Map> result = mongoTemplate.aggregate(
                                        aggregation, "weather", Map.class);

                        if (result.getMappedResults().isEmpty()) {
                                log.warn("{}에 대한 집계 결과가 없습니다. 수동 계산 시도...", region);
                                return calculateAverageManually(region, startTime, endTime);
                        }

                        Map<String, Object> avgResult = result.getMappedResults().get(0);
                        if (avgResult != null && avgResult.containsKey("avgTemperature")) {
                                Object avgTemp = avgResult.get("avgTemperature");
                                if (avgTemp != null) {
                                        double avgTemperature = Double.parseDouble(avgTemp.toString());
                                        log.info("지역 {} 평균 기온 계산 결과: {}", region, avgTemperature);
                                        return avgTemperature;
                                }
                        }

                        log.warn("{}에 대한 평균 기온 데이터가 없습니다. 수동 계산 시도...", region);
                        return calculateAverageManually(region, startTime, endTime);
                } catch (Exception e) {
                        log.error("평균 기온 계산 중 오류: {}", e.getMessage(), e);
                        return 0.0; // 오류 발생 시 0 반환
                }
        }

        // 집계 쿼리가 실패할 경우 수동으로 평균 계산
        private double calculateAverageManually(String region, String startTime, String endTime) {
                try {
                        log.info("수동 평균 계산 시작: 지역={}, 시작={}, 종료={}", region, startTime, endTime);

                        Query query = new Query(Criteria.where("region").is(region)
                                        .and("dateTime").gte(startTime).lte(endTime));

                        List<Weather> weatherList = mongoTemplate.find(query, Weather.class, "weather");
                        log.info("수동 계산: 조회된 데이터 개수: {}", weatherList.size());

                        if (weatherList.isEmpty()) {
                                log.warn("조건에 맞는 데이터가 없습니다");
                                return 0.0;
                        }

                        double sum = 0;
                        int count = 0;
                        for (Weather weather : weatherList) {
                                if (weather.getTemperature() != null && !weather.getTemperature().isEmpty()) {
                                        try {
                                                sum += Double.parseDouble(weather.getTemperature());
                                                count++;
                                        } catch (NumberFormatException e) {
                                                log.warn("온도 값 변환 실패: {}", weather.getTemperature());
                                        }
                                }
                        }

                        if (count > 0) {
                                double avg = sum / count;
                                log.info("수동 계산 결과: 총 {}개 데이터, 평균 기온: {}", count, avg);
                                return avg;
                        } else {
                                log.warn("유효한 온도 데이터가 없습니다.");
                                return 0.0;
                        }
                } catch (Exception e) {
                        log.error("수동 평균 계산 중 오류: {}", e.getMessage(), e);
                        return 0.0;
                }
        }
}