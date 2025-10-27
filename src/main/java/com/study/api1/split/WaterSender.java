package com.study.api1.split;

public class WaterSender {
    String id;
    String name;

    public WaterSender() {

    }

    public WaterSender(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "WaterSender{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}