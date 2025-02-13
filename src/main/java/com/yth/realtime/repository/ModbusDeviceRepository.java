package com.yth.realtime.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.yth.realtime.entity.ModbusDeviceDocument;

public interface ModbusDeviceRepository extends MongoRepository<ModbusDeviceDocument, String> {
    Optional<ModbusDeviceDocument> findByDeviceId(String deviceId);
    boolean existsByDeviceId(String deviceId);
    long deleteByDeviceId(String deviceId);
    List<ModbusDeviceDocument> findByActive(boolean active);
    List<ModbusDeviceDocument> findAll();
} 