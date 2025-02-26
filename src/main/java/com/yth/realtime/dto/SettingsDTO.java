package com.yth.realtime.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SettingsDTO {
    private SensorSettingsDTO temperature;
    private SensorSettingsDTO humidity;

    @Getter
    @Setter
    public static class SensorSettingsDTO {
        private int warningLow;
        private int dangerLow;
        private int normal;
        private int warningHigh;
        private int dangerHigh;
    }
}