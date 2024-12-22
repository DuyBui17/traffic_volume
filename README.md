import java.util.*;

// TrafficAnalysisByWeather Class: Analyzes traffic volume based on weather conditions.
public class TrafficAnalysisByWeather {

    // Inner class to represent a single traffic data record
    static class TrafficRecord {
        double trafficVolume;
        String weatherCondition;

        public TrafficRecord(double trafficVolume, String weatherCondition) {
            this.trafficVolume = trafficVolume;
            this.weatherCondition = weatherCondition;
        }
    }

    // Method to analyze traffic volume based on weather conditions
    public static Map<String, Double> analyzeTrafficByWeather(List<TrafficRecord> records) {
        Map<String, Double> weatherTrafficMap = new HashMap<>();
        Map<String, Integer> weatherCountMap = new HashMap<>();

        // Aggregate traffic volume and count records for each weather condition
        for (TrafficRecord record : records) {
            weatherTrafficMap.put(record.weatherCondition, 
                weatherTrafficMap.getOrDefault(record.weatherCondition, 0.0) + record.trafficVolume);
            weatherCountMap.put(record.weatherCondition, 
                weatherCountMap.getOrDefault(record.weatherCondition, 0) + 1);
        }

        // Calculate the average traffic volume for each weather condition
        for (String weather : weatherTrafficMap.keySet()) {
            double totalTraffic = weatherTrafficMap.get(weather);
            int count = weatherCountMap.get(weather);
            weatherTrafficMap.put(weather, totalTraffic / count);
        }

        return weatherTrafficMap;
    }

    // Main method for testing
    public static void main(String[] args) {
        // Sample data
        List<TrafficRecord> records = Arrays.asList(
            new TrafficRecord(5545, "Clouds"),
            new TrafficRecord(4516, "Clouds"),
            new TrafficRecord(4767, "Rain"),
            new TrafficRecord(5026, "Clouds"),
            new TrafficRecord(3000, "Snow")
        );

        // Perform analysis
        Map<String, Double> result = analyzeTrafficByWeather(records);

        // Display results
        System.out.println("Traffic Volume by Weather Condition:");
        for (Map.Entry<String, Double> entry : result.entrySet()) {
            System.out.printf("%s: %.2f\n", entry.getKey(), entry.getValue());
        }
    }
}
