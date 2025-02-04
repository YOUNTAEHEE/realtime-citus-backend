package com.yth.realtime.repository;

import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.yth.realtime.entity.Weather;

public interface WeatherRepository extends MongoRepository<Weather, String> {
    @Query(value = "{'region': ?0, 'dateTime': {$gte: ?1, $lte: ?2}}", sort = "{'dateTime': -1}")
    List<Weather> findByRegionAndDateTimeBetweenOrderByDateTimeDesc(
        String region, 
        String startDateTime,
        String endDateTime
    );

    // 날짜별 데이터 수를 확인하기 위한 메서드 추가
    @Query(value = "{'region': ?0}", 
           fields = "{'dateTime': 1}")
    List<Weather> findAllByRegion(String region);

    List<Weather> findByRegionAndDateTimeBetweenAndTemperatureNotNullAndTemperatureNotEmpty(
        String region, 
        String startDateTime, 
        String endDateTime, 
        Sort sort
    );
}