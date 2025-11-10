package com.study.api1.transformation;

public class WaterSensor {
    private String id;
    private String ts;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTs() {
        return ts;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                '}';
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
    public WaterSensor(String id, String ts) {
        this.id = id;
        this.ts = ts;
    }
    public WaterSensor() {
    }
}
