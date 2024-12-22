import java.util.*;
import java.text.*;
import java.util.stream.*;

public class TrafficAnalysisHoliday {
    private List<TrafficData> data;

    public TrafficAnalysisHoliday(List<TrafficData> data) {
        this.data = data;
    }

    // Phân tích lưu lượng giao thông trong các ngày lễ và không lễ
    public void analyzeTrafficOnHolidays() {
        long holidayCount = data.stream().filter(d -> d.getHoliday().equals("Yes")).count();
        long nonHolidayCount = data.stream().filter(d -> d.getHoliday().equals("No")).count();

        double holidayAvg = data.stream()
                .filter(d -> d.getHoliday().equals("Yes"))
                .mapToDouble(TrafficData::getTrafficVolume)
                .average()
                .orElse(0.0);

        double nonHolidayAvg = data.stream()
                .filter(d -> d.getHoliday().equals("No"))
                .mapToDouble(TrafficData::getTrafficVolume)
                .average()
                .orElse(0.0);

        System.out.println("Number of holidays: " + holidayCount);
        System.out.println("Number of non-holidays: " + nonHolidayCount);
        System.out.println("Average traffic on holidays: " + holidayAvg);
        System.out.println("Average traffic on non-holidays: " + nonHolidayAvg);
    }

    // Phân tích lưu lượng giao thông theo ngày trong tuần (so sánh giữa ngày lễ và không lễ)
    public void visualizeTrafficByDay() {
        Map<String, Double> holidayAvgTrafficByDay = new HashMap<>();
        Map<String, Double> nonHolidayAvgTrafficByDay = new HashMap<>();

        data.forEach(d -> {
            if (d.getHoliday().equals("Yes")) {
                holidayAvgTrafficByDay.merge(d.getDayOfWeek(), d.getTrafficVolume(), Double::sum);
            } else {
                nonHolidayAvgTrafficByDay.merge(d.getDayOfWeek(), d.getTrafficVolume(), Double::sum);
            }
        });

        // Tính trung bình theo ngày
        holidayAvgTrafficByDay.forEach((day, totalTraffic) ->
                holidayAvgTrafficByDay.put(day, totalTraffic / countByDay("Yes", day))
        );

        nonHolidayAvgTrafficByDay.forEach((day, totalTraffic) ->
                nonHolidayAvgTrafficByDay.put(day, totalTraffic / countByDay("No", day))
        );

        System.out.println("Holiday traffic by day of the week: " + holidayAvgTrafficByDay);
        System.out.println("Non-holiday traffic by day of the week: " + nonHolidayAvgTrafficByDay);
    }

    // So sánh lưu lượng giao thông theo giờ trong ngày
    public void compareTrafficByHour() {
        Map<Integer, Double> holidayAvgTrafficByHour = new HashMap<>();
        Map<Integer, Double> nonHolidayAvgTrafficByHour = new HashMap<>();

        data.forEach(d -> {
            if (d.getHoliday().equals("Yes")) {
                holidayAvgTrafficByHour.merge(d.getHour(), d.getTrafficVolume(), Double::sum);
            } else {
                nonHolidayAvgTrafficByHour.merge(d.getHour(), d.getTrafficVolume(), Double::sum);
            }
        });

        // Tính trung bình theo giờ
        holidayAvgTrafficByHour.forEach((hour, totalTraffic) ->
                holidayAvgTrafficByHour.put(hour, totalTraffic / countByHour("Yes", hour))
        );

        nonHolidayAvgTrafficByHour.forEach((hour, totalTraffic) ->
                nonHolidayAvgTrafficByHour.put(hour, totalTraffic / countByHour("No", hour))
        );

        System.out.println("Holiday traffic by hour: " + holidayAvgTrafficByHour);
        System.out.println("Non-holiday traffic by hour: " + nonHolidayAvgTrafficByHour);
    }

    // Đếm số lượng ngày lễ hoặc không lễ cho một ngày cụ thể trong tuần
    private long countByDay(String holidayType, String dayOfWeek) {
        return data.stream().filter(d -> d.getHoliday().equals(holidayType) && d.getDayOfWeek().equals(dayOfWeek)).count();
    }

    // Đếm số lượng giờ lễ hoặc không lễ cho một giờ cụ thể trong ngày
    private long countByHour(String holidayType, int hour) {
        return data.stream().filter(d -> d.getHoliday().equals(holidayType) && d.getHour() == hour).count();
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

    public static void main(String[] args) throws ParseException {
        List<TrafficData> data = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");

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
