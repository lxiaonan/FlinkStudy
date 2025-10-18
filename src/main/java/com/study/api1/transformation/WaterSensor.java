package com.study.api1.transformation;

public class WaterSensor {
    private String id;
    private String age;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", age=" + age +
                '}';
    }

    public void setAge(String age) {
        this.age = age;
    }
    public WaterSensor(String id, String age) {
        this.id = id;
        this.age = age;
    }
    public WaterSensor() {
    }
}
