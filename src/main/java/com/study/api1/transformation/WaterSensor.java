package com.study.api1.transformation;

public class WaterSensor {
    private String id;
    private String ts;
    private Integer vc;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTs() {
        return ts;
    }
    public Integer getVc() {
        return vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts='" + ts + '\'' +
                ", vc='" + vc + '\'' +
                '}';
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
    public WaterSensor(String id, String ts) {
        this.id = id;
        this.ts = ts;
    }
    public WaterSensor(String id, String ts,Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }
    public WaterSensor() {
    }
}
