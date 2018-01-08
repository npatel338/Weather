package com.capitalone.weathertracker.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;

import javax.ws.rs.core.Response;
import java.util.*;

public class WeatherTrackerService {

    private DataUtility database = DataUtility.getInstance();

    public static final Response CONFLICT = Response.status(409).build();
    public static final Response NOT_FOUND = Response.status(404).build();
    public static final Response BAD_DATA = Response.status(400).build();
    public static final Response UPDATED = Response.status(204).build();
    public Response saveData(JsonNode measurement) {

        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        String key = null;
        Map<String, Float> tempMap = new HashMap<>();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            if (field.equalsIgnoreCase("timestamp")) {
                key = entry.getValue().textValue();
            } else {
                if (entry.getValue().textValue() != null) {
                    return BAD_DATA;
                }
                Float value = entry.getValue().floatValue();
                tempMap.put(field, value);
            }
        }
        if (key != null) {
            database.put(key, tempMap);
        } else {
            return BAD_DATA;
        }
        return Response.status(201).header("Location", "/measurements/" + key).build();
    }

    public List<Map<String, Object>> retrieveDataTimeStampBased(String timestamp) {

        Map<String, Map<String, Float>> allData = database.getData();

        List<Map<String, Object>> measurements = new ArrayList<>();

        for (Map.Entry<String, Map<String, Float>> entry : allData.entrySet()) {
            if (entry.getKey().startsWith(timestamp)) {
                Map<String, Object> measurement = new HashMap<>();
                measurement.put("timestamp", entry.getKey());
                Iterator it = entry.getValue().entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    measurement.put(pair.getKey().toString(), Float.parseFloat(pair.getValue().toString()));
                }
                measurements.add(measurement);
            }
        }

        Comparator<Map<String, Object>> mapComparator = new Comparator<Map<String, Object>>() {
            public int compare(Map<String, Object> m1, Map<String, Object> m2) {
                String m1Timestamp = m1.get("timestamp").toString();
                String m2Timestamp = m2.get("timestamp").toString();

                return m1Timestamp.compareTo(m2Timestamp);
            }
        };

        Collections.sort(measurements, mapComparator);

        return measurements;

    }


    public Response updateMeasurement(String timestamp, JsonNode measurement) {
        Map<String, Map<String, Float>> allData = database.getData();

        //get the values of the map from the measurements
        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        Map<String, Float> tempMap = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            if (!field.equalsIgnoreCase("timestamp")) {
                if (entry.getValue().textValue() != null) {
                    return BAD_DATA;
                }
                Float value = entry.getValue().floatValue();
                tempMap.put(field, value);
            } else {
                if (!entry.getValue().textValue().equalsIgnoreCase(timestamp)) {
                    return CONFLICT;
                }
            }
        }

        if (!allData.keySet().contains(timestamp)) {
            return NOT_FOUND;
        }

        allData.put(timestamp, tempMap);
        return UPDATED;
    }

    public Response patchMeasurement(String timestamp, JsonNode measurement) {

        Map<String, Map<String, Float>> allData = database.getData();
        Map<String, Float> existingMeasurement = allData.get(timestamp);

        if (existingMeasurement == null) {
            return NOT_FOUND;
        }

        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        String key;

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            if (field.equalsIgnoreCase("timestamp")) {
                key = entry.getValue().textValue();
                if (!key.equalsIgnoreCase(timestamp)) {
                    return CONFLICT;
                }
            } else {
                if (entry.getValue().textValue() != null) {
                    return BAD_DATA;
                }
                Float value = entry.getValue().floatValue();
                existingMeasurement.put(field, value);
            }
        }

        allData.put(timestamp, existingMeasurement);
        return UPDATED;
    }

    public Response deleteMeasurement(String timestamp) {
        Map<String, Map<String, Float>> allData = database.getData();
        Map<String, Float> existingMeasurement = allData.get(timestamp);
        if (existingMeasurement == null) {
            return NOT_FOUND;
        }
        database.delete(timestamp);
        return UPDATED;
    }

    public List<Map<String, Object>> getStat(List<String> metrics, List<String> stats, String fromDateTime, String toDateTime) {

        List<Map<String, Object>> statsList = new ArrayList<>();

        Map<String, Map<String, Float>> allData = database.getData();

        Map<String, Float> variableStats = new HashMap<>();

        for (String metric : metrics) {
            if (stats.contains("min")) {
                variableStats.put(metric + "min", 1000.0f);
                variableStats.put(metric + "min" + "include", 0.0f);
            }
            if (stats.contains("max")) {
                variableStats.put(metric + "max", 0.0f);
                variableStats.put(metric + "max" + "include", 0.0f);
            }
            if (stats.contains("average")) {
                variableStats.put(metric + "average" + "sum", 0.0f);
                variableStats.put(metric + "average" + "count", 0.0f);
                variableStats.put(metric + "average" + "include", 0.0f);
            }
        }

        for (Map.Entry<String, Map<String, Float>> entry : allData.entrySet()) {

            DateTime measurementDate = new DateTime(entry.getKey());

            if (fromDateTime == null && toDateTime == null) {
                calculateStat(entry, metrics, stats, variableStats);
            } else if (fromDateTime != null && toDateTime != null) {
                DateTime fromDate = new DateTime(fromDateTime);
                DateTime toDate = new DateTime(toDateTime);
                if ((measurementDate.isEqual(fromDate) || measurementDate.isAfter(fromDate)) && measurementDate.isBefore(toDate)) {
                    calculateStat(entry, metrics, stats, variableStats);
                }
            } else if (fromDateTime != null) {
                DateTime fromDate = new DateTime(fromDateTime);
                if (measurementDate.isEqual(fromDate) || measurementDate.isAfter(fromDate)) {
                    calculateStat(entry, metrics, stats, variableStats);
                }
            } else {
                DateTime toDate = new DateTime(toDateTime);
                if (measurementDate.isBefore(toDate)) {
                    calculateStat(entry, metrics, stats, variableStats);
                }
            }
        }

        for (String metric : metrics) {
            if (stats.contains("min") && (variableStats.get(metric + "min" + "include") != 0f)) {
                Map<String, Object> minMap = new HashMap<>();
                minMap.put("metric", metric);
                minMap.put("stat", "min");
                minMap.put("value", variableStats.get(metric + "min"));
                statsList.add(minMap);
            }
            if (stats.contains("max") && (variableStats.get(metric + "max" + "include") != 0f)) {
                Map<String, Object> maxMap = new HashMap<>();
                maxMap.put("metric", metric);
                maxMap.put("stat", "max");
                maxMap.put("value", variableStats.get(metric + "max"));
                statsList.add(maxMap);
            }
            if (stats.contains("average") && (variableStats.get(metric + "average" + "include") != 0f)) {
                Float average = variableStats.get(metric + "average" + "sum") / variableStats.get(metric + "average" + "count");
                average = Math.round(average * 10.0f) / 10.0f;
                Map<String, Object> averageMap = new HashMap<>();
                averageMap.put("metric", metric);
                averageMap.put("stat", "average");
                averageMap.put("value", average);
                statsList.add(averageMap);
            }
        }

        return statsList;
    }

    private void calculateStat(Map.Entry<String, Map<String, Float>> entry, List<String> metric, List<String> stats, Map<String, Float> variableStats) {
        Map<String, Float> measurementValues = entry.getValue();
        //run through the keySet and see if it contains a particular metric
        for (Map.Entry<String, Float> entry1 : measurementValues.entrySet()) {
            if (metric.contains(entry1.getKey())) {
                if (stats.contains("min")) {
                    if (entry1.getValue() < variableStats.get(entry1.getKey() + "min")) {
                        variableStats.put(entry1.getKey() + "min", entry1.getValue());
                        variableStats.put(entry1.getKey() + "min" + "include", 1f);
                    }
                }
                if (stats.contains("max")) {
                    if (entry1.getValue() > variableStats.get(entry1.getKey() + "max")) {
                        variableStats.put(entry1.getKey() + "max", entry1.getValue());
                        variableStats.put(entry1.getKey() + "max" + "include", 1f);
                    }
                }
                if (stats.contains("average")) {
                    Float newSum = entry1.getValue() + variableStats.get(entry1.getKey() + "average" + "sum");
                    variableStats.put(entry1.getKey() + "average" + "sum", newSum);
                    variableStats.put(entry1.getKey() + "average" + "count", variableStats.get(entry1.getKey() + "average" + "count") + 1);
                    variableStats.put(entry1.getKey() + "average" + "include", 1f);

                }
            }
        }
    }
}