package com.capitalone.weathertracker.services;

import java.util.HashMap;
import java.util.Map;

public class DataUtility {

    private static DataUtility instance = null;

    private DataUtility() {
    }

    public static DataUtility getInstance() {
        if (instance == null) {
            instance = new DataUtility();
        }
        return instance;
    }

    private Map<String, Map<String, Float>> data = new HashMap<>();

    public Map<String, Map<String, Float>> getData() {
        return data;
    }

    public void put(String key, Map<String, Float> value) {
        data.put(key, value);
    }

    public void delete(String key) {
        data.remove(key);
    }
}