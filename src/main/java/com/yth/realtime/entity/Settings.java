package com.yth.realtime.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Getter;
import lombok.Setter;

@Document(collection = "settings")
@Getter
@Setter
public class Settings {
    @Id
    private String id;

    private String type;
    private int warningLow;
    private int dangerLow;
    private int normal;
    private int warningHigh;
    private int dangerHigh;
}