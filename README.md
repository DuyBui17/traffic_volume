import java.io.*;
import java.util.*;

public class TemperatureConverterFromCSV {

    // Phương thức chuyển đổi từ Fahrenheit sang Celsius
    public static double fahrenheitToCelsius(double fahrenheit) {
        return (fahrenheit - 32) * 5 / 9;
    }

    // Lớp chứa thông tin về dữ liệu giao thông và thời tiết
    public static class TrafficWeatherData {
        private int trafficVolume;
        private String holiday;
        private double tempFahrenheit;
        private double rain1h;
        private double snow1h;
        private int cloudsAll;
        private String weatherMain;
        private String weatherDescription;
        private String dateTime;

        public TrafficWeatherData(int trafficVolume, String holiday, double tempFahrenheit, double rain1h, double snow1h, int cloudsAll, String weatherMain, String weatherDescription, String dateTime) {
            this.trafficVolume = trafficVolume;
            this.holiday = holiday;
            this.tempFahrenheit = tempFahrenheit;
            this.rain1h = rain1h;
            this.snow1h = snow1h;
            this.cloudsAll = cloudsAll;
            this.weatherMain = weatherMain;
            this.weatherDescription = weatherDescription;
            this.dateTime = dateTime;
        }

        public String getDateTime() {
            return dateTime;
        }

        public int getTrafficVolume() {
            return trafficVolume;
        }

        public String getHoliday() {
            return holiday;
        }

        public double getTempFahrenheit() {
            return tempFahrenheit;
        }

        public double getTempCelsius() {
            return fahrenheitToCelsius(tempFahrenheit);
        }

        public double getRain1h() {
            return rain1h;
        }

        public double getSnow1h() {
            return snow1h;
        }

        public int getCloudsAll() {
            return cloudsAll;
        }

        public String getWeatherMain() {
            return weatherMain;
        }

        public String getWeatherDescription() {
            return weatherDescription;
        }
    }

    // Phương thức ghi dữ liệu vào file CSV
    public static void writeDataToCSV(List<TrafficWeatherData> data, String filename) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Ghi tiêu đề
            writer.write("traffic_volume,holiday,temp_Fahrenheit,temp_Celsius,rain_1h,snow_1h,clouds_all,weather_main,weather_description,date_time\n");

            // Ghi dữ liệu giao thông và thời tiết đã chuyển đổi
            for (TrafficWeatherData entry : data) {
                writer.write(entry.getTrafficVolume() + "," +
                             entry.getHoliday() + "," +
                             entry.getTempFahrenheit() + "," +
                             entry.getTempCelsius() + "," +
                             entry.getRain1h() + "," +
                             entry.getSnow1h() + "," +
                             entry.getCloudsAll() + "," +
                             entry.getWeatherMain() + "," +
                             entry.getWeatherDescription() + "," +
                             entry.getDateTime() + "\n");
            }

            System.out.println("Data written to " + filename);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    // Phương thức đọc dữ liệu từ file CSV và chuyển đổi nhiệt độ
    public static List<TrafficWeatherData> readDataFromCSV(String filename) {
        List<TrafficWeatherData> dataList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            reader.readLine(); // Bỏ qua dòng tiêu đề

            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length == 9) {
                    // Chuyển đổi và tạo đối tượng TrafficWeatherData
                    int trafficVolume = Integer.parseInt(values[0]);
                    String holiday = values[1];
                    double tempFahrenheit = Double.parseDouble(values[2]);
                    double rain1h = Double.parseDouble(values[3]);
                    double snow1h = Double.parseDouble(values[4]);
                    int cloudsAll = Integer.parseInt(values[5]);
                    String weatherMain = values[6];
                    String weatherDescription = values[7];
                    String dateTime = values[8];

                    // Thêm vào danh sách dữ liệu
                    dataList.add(new TrafficWeatherData(trafficVolume, holiday, tempFahrenheit, rain1h, snow1h, cloudsAll, weatherMain, weatherDescription, dateTime));
                }
            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        return dataList;
    }

    public static void main(String[] args) {
        // Dữ liệu mẫu (Có thể thay đổi bằng việc đọc từ file CSV thực tế)
        List<TrafficWeatherData> data = new ArrayList<>();
        data.add(new TrafficWeatherData(5545, "None", 288.28, 0, 0, 40, "Clouds", "scattered clouds", "02-10-2012 09:00"));
        data.add(new TrafficWeatherData(4516, "None", 289.36, 0, 0, 75, "Clouds", "broken clouds", "02-10-2012 10:00"));
        data.add(new TrafficWeatherData(4767, "None", 289.58, 0, 0, 90, "Clouds", "overcast clouds", "02-10-2012 11:00"));
        data.add(new TrafficWeatherData(5026, "None", 290.13, 0, 0, 90, "Clouds", "overcast clouds", "02-10-2012 12:00"));

        // Ghi dữ liệu vào file CSV
        writeDataToCSV(data, "converted_temperature_data.csv");
    }
}
