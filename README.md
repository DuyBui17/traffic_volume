import java.util.*;

// TrafficAnalysisByWeather Class: Analyzes traffic volume based on weather conditions.
public class TrafficAnalysisByWeather {

    // Inner class to represent a single traffic data record
    static class TrafficRecord {
        private double trafficVolume;
        private String weatherCondition;

        public TrafficRecord(double trafficVolume, String weatherCondition) {
            this.trafficVolume = trafficVolume;
            this.weatherCondition = weatherCondition;
        }

        public double getTrafficVolume() {
            return trafficVolume;
        }

        public String getWeatherCondition() {
            return weatherCondition;
        }
    }

    // Method to analyze traffic volume based on weather conditions
    public static Map<String, Double> analyzeTrafficByWeather(List<TrafficRecord> records) {
        Map<String, Double> totalTrafficByWeather = new HashMap<>();
        Map<String, Integer> countByWeather = new HashMap<>();

        // Aggregate traffic volume and count records for each weather condition
        for (TrafficRecord record : records) {
            String weather = record.getWeatherCondition();
            double volume = record.getTrafficVolume();

            // Update totalTrafficByWeather
            if (!totalTrafficByWeather.containsKey(weather)) {
                totalTrafficByWeather.put(weather, volume);
            } else {
                totalTrafficByWeather.put(weather, totalTrafficByWeather.get(weather) + volume);
            }

            // Update countByWeather
            if (!countByWeather.containsKey(weather)) {
                countByWeather.put(weather, 1);
            } else {
                countByWeather.put(weather, countByWeather.get(weather) + 1);
            }
        }

        // Calculate the average traffic volume for each weather condition
        Map<String, Double> averageTrafficByWeather = new HashMap<>();
        for (String weather : totalTrafficByWeather.keySet()) {
            double totalTraffic = totalTrafficByWeather.get(weather);
            int count = countByWeather.get(weather);
            averageTrafficByWeather.put(weather, totalTraffic / count);
        }

        return averageTrafficByWeather;
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
