package com.yth.realtime.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.yth.realtime.entity.WeatherStation;

public interface WeatherStationRepository extends MongoRepository<WeatherStation, String> {
    boolean existsByStnId(String stnId);
} 