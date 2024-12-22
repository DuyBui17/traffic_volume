import java.util.*;
import java.text.*;

public class TrafficAnalysisHoliday {
    private List<TrafficData> data;

    public TrafficAnalysisHoliday(List<TrafficData> data) {
        this.data = data;
    }

    // Phân tích lưu lượng giao thông trong các ngày lễ và không lễ
    public void analyzeTrafficOnHolidays() {
        int holidayCount = 0;
        int nonHolidayCount = 0;
        double holidaySum = 0;
        double nonHolidaySum = 0;

        for (TrafficData d : data) {
            if (d.getHoliday().equals("Yes")) {
                holidayCount++;
                holidaySum += d.getTrafficVolume();
            } else {
                nonHolidayCount++;
                nonHolidaySum += d.getTrafficVolume();
            }
        }

        double holidayAvg = holidayCount > 0 ? holidaySum / holidayCount : 0;
        double nonHolidayAvg = nonHolidayCount > 0 ? nonHolidaySum / nonHolidayCount : 0;

        System.out.println("Number of holidays: " + holidayCount);
        System.out.println("Number of non-holidays: " + nonHolidayCount);
        System.out.println("Average traffic on holidays: " + holidayAvg);
        System.out.println("Average traffic on non-holidays: " + nonHolidayAvg);
    }

    // Phân tích lưu lượng giao thông theo ngày trong tuần (so sánh giữa ngày lễ và không lễ)
    public void visualizeTrafficByDay() {
        Map<String, Double> holidayAvgTrafficByDay = new HashMap<>();
        Map<String, Double> nonHolidayAvgTrafficByDay = new HashMap<>();
        Map<String, Integer> holidayDayCount = new HashMap<>();
        Map<String, Integer> nonHolidayDayCount = new HashMap<>();

        // Tính tổng và số lượng giao thông theo ngày trong tuần cho cả ngày lễ và không lễ
        for (TrafficData d : data) {
            String dayOfWeek = d.getDayOfWeek();
            if (d.getHoliday().equals("Yes")) {
                holidayAvgTrafficByDay.put(dayOfWeek, holidayAvgTrafficByDay.getOrDefault(dayOfWeek, 0.0) + d.getTrafficVolume());
                holidayDayCount.put(dayOfWeek, holidayDayCount.getOrDefault(dayOfWeek, 0) + 1);
            } else {
                nonHolidayAvgTrafficByDay.put(dayOfWeek, nonHolidayAvgTrafficByDay.getOrDefault(dayOfWeek, 0.0) + d.getTrafficVolume());
                nonHolidayDayCount.put(dayOfWeek, nonHolidayDayCount.getOrDefault(dayOfWeek, 0) + 1);
            }
        }

        // Tính trung bình giao thông theo ngày trong tuần
        for (String day : holidayAvgTrafficByDay.keySet()) {
            holidayAvgTrafficByDay.put(day, holidayAvgTrafficByDay.get(day) / holidayDayCount.get(day));
        }

        for (String day : nonHolidayAvgTrafficByDay.keySet()) {
            nonHolidayAvgTrafficByDay.put(day, nonHolidayAvgTrafficByDay.get(day) / nonHolidayDayCount.get(day));
        }

        System.out.println("Holiday traffic by day of the week: " + holidayAvgTrafficByDay);
        System.out.println("Non-holiday traffic by day of the week: " + nonHolidayAvgTrafficByDay);
    }

    // So sánh lưu lượng giao thông theo giờ trong ngày
    public void compareTrafficByHour() {
        Map<Integer, Double> holidayAvgTrafficByHour = new HashMap<>();
        Map<Integer, Double> nonHolidayAvgTrafficByHour = new HashMap<>();
        Map<Integer, Integer> holidayHourCount = new HashMap<>();
        Map<Integer, Integer> nonHolidayHourCount = new HashMap<>();

        // Tính tổng và số lượng giao thông theo giờ trong ngày cho cả ngày lễ và không lễ
        for (TrafficData d : data) {
            int hour = d.getHour();
            if (d.getHoliday().equals("Yes")) {
                holidayAvgTrafficByHour.put(hour, holidayAvgTrafficByHour.getOrDefault(hour, 0.0) + d.getTrafficVolume());
                holidayHourCount.put(hour, holidayHourCount.getOrDefault(hour, 0) + 1);
            } else {
                nonHolidayAvgTrafficByHour.put(hour, nonHolidayAvgTrafficByHour.getOrDefault(hour, 0.0) + d.getTrafficVolume());
                nonHolidayHourCount.put(hour, nonHolidayHourCount.getOrDefault(hour, 0) + 1);
            }
        }

        // Tính trung bình giao thông theo giờ trong ngày
        for (int hour : holidayAvgTrafficByHour.keySet()) {
            holidayAvgTrafficByHour.put(hour, holidayAvgTrafficByHour.get(hour) / holidayHourCount.get(hour));
        }

        for (int hour : nonHolidayAvgTrafficByHour.keySet()) {
            nonHolidayAvgTrafficByHour.put(hour, nonHolidayAvgTrafficByHour.get(hour) / nonHolidayHourCount.get(hour));
        }

        System.out.println("Holiday traffic by hour: " + holidayAvgTrafficByHour);
        System.out.println("Non-holiday traffic by hour: " + nonHolidayAvgTrafficByHour);
    }

    // Class lưu trữ thông tin về giao thông
    public static class TrafficData {
        private int trafficVolume;
        private String holiday;
        private String dayOfWeek;
        private int hour;

        public TrafficData(int trafficVolume, String holiday, String dayOfWeek, int hour) {
            this.trafficVolume = trafficVolume;
            this.holiday = holiday;
            this.dayOfWeek = dayOfWeek;
            this.hour = hour;
        }

        public int getTrafficVolume() {
            return trafficVolume;
        }

        public String getHoliday() {
            return holiday;
        }

        public String getDayOfWeek() {
            return dayOfWeek;
        }

        public int getHour() {
            return hour;
        }
    }

    public static void main(String[] args) {
        List<TrafficData> data = new ArrayList<>();

        // Ví dụ về dữ liệu
        data.add(new TrafficData(5545, "No", "Monday", 9));
        data.add(new TrafficData(4516, "No", "Monday", 10));
        data.add(new TrafficData(4767, "No", "Monday", 11));
        data.add(new TrafficData(5026, "No", "Monday", 12));
        
        // Khởi tạo đối tượng phân tích
        TrafficAnalysisHoliday analysis = new TrafficAnalysisHoliday(data);

        // Phân tích lưu lượng giao thông vào các ngày lễ và không lễ
        analysis.analyzeTrafficOnHolidays();
        
        // Phân tích lưu lượng giao thông theo ngày trong tuần
        analysis.visualizeTrafficByDay();
        
        // So sánh lưu lượng giao thông theo giờ
        analysis.compareTrafficByHour();
    }
}
