package com.capitalone.weathertracker.services;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

public class Test {

    private static Test instance = null;

    private Test() {
    }

    public static Test getInstance() {
        if (instance == null) {
            instance = new Test();
        }
        return instance;
    }

    private Map<String, Object> data = new HashMap<>();

    public Map<String, Object> getData() {
        return data;
    }

    public void put(String key, String value) {
        data.put(key, value);
    }

    public void put(String key, JsonNode value){
        data.put(key, value);
    }

    public String get(String key) {
        Object temp = data.get(key);
        if (temp != null) {
            return temp.toString();
        } else {
            return "no data";
        }
    }
}