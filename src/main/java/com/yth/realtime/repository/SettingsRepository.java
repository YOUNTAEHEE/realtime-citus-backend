package com.yth.realtime.repository;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.yth.realtime.entity.Settings;

@Repository
public interface SettingsRepository extends MongoRepository<Settings, String> {
    Optional<Settings> findByType(String type);
}