package com.capitalone.weathertracker.services;

import com.capitalone.weathertracker.resources.RootResource;
import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;

import javax.ws.rs.core.Response;
import java.text.ParseException;
import java.util.*;

public class WeatherTrackerService {

    private DataUtility database = DataUtility.getInstance();
    private Test testData = Test.getInstance();


    public Response testingService() {
        return Response
                .ok("WeatherTracker! \n")
                .build();
    }

    public void saveData(JsonNode measurement) {

        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        String key = null;
        Map<String, Float> tempMap = new HashMap<>();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            if (field.equalsIgnoreCase("timestamp")) {
                key = entry.getValue().textValue();
            } else {
                Float value = entry.getValue().floatValue();
                tempMap.put(field, value);
            }
        }
        if (key != null) {
            database.put(key, tempMap);
        }
    }

    public List<Map<String, Object>> retrieveDataTimeStampBased(String timestamp) throws ParseException {

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

        return measurements;

    }

    public void updateMeasurement(String timestamp, JsonNode measurement) {
        Map<String, Map<String, Float>> allData = database.getData();

        //get the values of the map from the measurements
        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        Map<String, Float> tempMap = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            JsonNode value = entry.getValue();
            if (!field.equalsIgnoreCase("timestamp")) {
                tempMap.put(field, Float.parseFloat(value.textValue()));
            }
        }
        allData.put(timestamp, tempMap);
    }

    public Response patchMeasurement(String timestamp, JsonNode measurement) {

        Map<String, Map<String, Float>> allData = database.getData();
        Map<String, Float> existingMeasurement = allData.get(timestamp);

        if (existingMeasurement == null) {
            return RootResource.NOT_FOUND;
        }

        Iterator<Map.Entry<String, JsonNode>> iterator = measurement.fields();
        String key;

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String field = entry.getKey();
            JsonNode value = entry.getValue();
            if (field.equalsIgnoreCase("timestamp")) {
                key = entry.getValue().textValue();
                if (!key.equalsIgnoreCase(timestamp)) {
                    return RootResource.CONFLICT;
                }
            } else {
                //check for the dataType of the values
                if (existingMeasurement.get(field) != null) {
                    existingMeasurement.put(field, Float.parseFloat(value.textValue()));
                }
            }
        }

        allData.put(timestamp, existingMeasurement);
        return RootResource.SUCCESSFUL;
    }

    public void deleteMeasurement(String timestamp) {
        database.delete(timestamp);
    }

    public List<Map<String, Object>> getStat(List<String> metric, List<String> stats, String fromDateTime, String toDateTime) {

        List<Map<String, Object>> statsList = new ArrayList<>();

        Map<String, Map<String, Float>> allData = database.getData();

        Map<String, Float> variableStats = new HashMap<>();

        for (String met : metric) {
            if (stats.contains("min")) {
                variableStats.put(met + "min", 1000.0f);
            }
            if (stats.contains("max")) {
                variableStats.put(met + "max", 0.0f);
            }
            if (stats.contains("average")) {
                variableStats.put(met + "average" + "sum", 0.0f);
                variableStats.put(met + "average" + "count", 0.0f);
            }
        }

        //looping through the data
        for (Map.Entry<String, Map<String, Float>> entry : allData.entrySet()) {

            DateTime measurementDate = new DateTime(entry.getKey());

            if (fromDateTime == null && toDateTime == null) {
                calculateStat(entry, metric, stats, variableStats);
            } else if (fromDateTime != null && toDateTime != null) {
                DateTime fromDate = new DateTime(fromDateTime);     // TODO: 1/7/18 followup on this
                DateTime toDate = new DateTime(toDateTime);
                if ((measurementDate.isEqual(fromDate) || measurementDate.isAfter(fromDate)) && (measurementDate.isEqual(toDate) || measurementDate.isBefore(toDate))) {
                    calculateStat(entry, metric, stats, variableStats);
                }
            } else if (fromDateTime != null) {
                DateTime fromDate = new DateTime(fromDateTime);
                if (measurementDate.isEqual(fromDate) || measurementDate.isAfter(fromDate)) {
                    calculateStat(entry, metric, stats, variableStats);
                }
            } else if (toDateTime != null) {
                DateTime toDate = new DateTime(toDateTime);
                if (measurementDate.isEqual(toDate) || measurementDate.isBefore(toDate)) {
                    calculateStat(entry, metric, stats, variableStats);
                }
            }
        }

        for (String met : metric) {
            if (stats.contains("min")) {
                Map<String, Object> minMap = new HashMap<>();
                minMap.put("metric", met);
                minMap.put("stat", "min");
                minMap.put("value", variableStats.get(met + "min"));
                statsList.add(minMap);
            }
            if (stats.contains("max")) {
                Map<String, Object> maxMap = new HashMap<>();
                maxMap.put("metric", met);
                maxMap.put("stat", "max");
                maxMap.put("value", variableStats.get(met + "max"));
                statsList.add(maxMap);
            }
            if (stats.contains("average")) {
                Map<String, Object> averageMap = new HashMap<>();
                averageMap.put("metric", met);
                averageMap.put("stat", "average");
                averageMap.put("value", variableStats.get(met + "average" + "sum") / variableStats.get(met + "average" + "count"));
                statsList.add(averageMap);
            }
        }

        return statsList;
    }

    public void calculateStat(Map.Entry<String, Map<String, Float>> entry, List<String> metric, List<String> stats, Map<String, Float> variableStats) {
        Map<String, Float> measurementValues = entry.getValue();
        //run through the keySet and see if it contains a particular metric
        for (Map.Entry<String, Float> entry1 : measurementValues.entrySet()) {
            if (metric.contains(entry1.getKey())) {
                if (stats.contains("min")) {
                    if (entry1.getValue() < variableStats.get(entry1.getKey() + "min")) {
                        variableStats.put(entry1.getKey() + "min", entry1.getValue());
                    }
                }
                if (stats.contains("max")) {
                    if (entry1.getValue() > variableStats.get(entry1.getKey() + "max")) {
                        variableStats.put(entry1.getKey() + "max", entry1.getValue());
                    }
                }
                if (stats.contains("average")) {
                    Float newSum = entry1.getValue() + variableStats.get(entry1.getKey() + "average" + "sum");
                    variableStats.put(entry1.getKey() + "average" + "sum", newSum);
                    variableStats.put(entry1.getKey() + "average" + "count", variableStats.get(entry1.getKey() + "average" + "count") + 1);
                }
            }
        }
    }

    public void saveValuesTest(JsonNode values) {
        Iterator<Map.Entry<String, JsonNode>> iterator = values.fields();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            testData.put(key, value);
        }

    }


    public String retrieveTestData(String key) {
        Map<String, Object> sampleData = testData.getData();
        Object temp = sampleData.get(key);
        if (temp != null) {
            return temp.toString();
        }
        return "Did not find the data";
    }

    public Map<String, Object> retrieveTestDataAll() {
        return testData.getData();
    }
}