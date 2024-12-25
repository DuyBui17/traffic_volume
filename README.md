import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.*;

public class TrafficWeatherAnalysis {

    public static void analyzeTrafficWeather(String inputFilePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Đọc file đầu vào từ HDFS
        Path inputPath = new Path(inputFilePath);
        FSDataInputStream in = fs.open(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;
        Map<String, List<Double>> trafficData = new HashMap<>();
        int lineCount = 0;

        // Đọc từng dòng dữ liệu
        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");

            // Bỏ qua dòng đầu tiên (header)
            if (lineCount == 0) {
                lineCount++;
                continue;
            }

            try {
                double trafficVolume = Double.parseDouble(data[0]);  // Lưu lượng giao thông
                double temp = Double.parseDouble(data[2]);  // Nhiệt độ (đã chuyển sang °C)
                double rain = Double.parseDouble(data[3]);  // Lượng mưa
                double snow = Double.parseDouble(data[4]);  // Lượng tuyết
                double clouds = Double.parseDouble(data[5]);  // Mây

                // Kiểm tra và thêm giá trị vào bản đồ (Map) nếu chưa tồn tại
                if (temp != -1) {
                    if (!trafficData.containsKey("temp")) {
                        trafficData.put("temp", new ArrayList<>());
                    }
                    trafficData.get("temp").add(temp);
                }

                if (rain != -1) {
                    if (!trafficData.containsKey("rain")) {
                        trafficData.put("rain", new ArrayList<>());
                    }
                    trafficData.get("rain").add(rain);
                }

                if (snow != -1) {
                    if (!trafficData.containsKey("snow")) {
                        trafficData.put("snow", new ArrayList<>());
                    }
                    trafficData.get("snow").add(snow);
                }

                if (clouds != -1) {
                    if (!trafficData.containsKey("clouds")) {
                        trafficData.put("clouds", new ArrayList<>());
                    }
                    trafficData.get("clouds").add(clouds);
                }

                if (trafficVolume != -1) {
                    if (!trafficData.containsKey("trafficVolume")) {
                        trafficData.put("trafficVolume", new ArrayList<>());
                    }
                    trafficData.get("trafficVolume").add(trafficVolume);
                }

            } catch (NumberFormatException e) {
                // Bỏ qua các dòng dữ liệu không hợp lệ
                continue;
            }

            lineCount++;
        }

        br.close();
        in.close();

        // Tính các thống kê cơ bản và phân tích mối quan hệ giữa thời tiết và giao thông
        System.out.println("Phân tích ảnh hưởng của thời tiết đến giao thông:");
        analyzeCorrelation(trafficData);
    }

    // Hàm tính toán mối quan hệ giữa các yếu tố thời tiết và lưu lượng giao thông
    public static void analyzeCorrelation(Map<String, List<Double>> trafficData) {
        List<Double> trafficVolume = trafficData.get("trafficVolume");
        List<Double> temp = trafficData.get("temp");
        List<Double> rain = trafficData.get("rain");
        List<Double> snow = trafficData.get("snow");
        List<Double> clouds = trafficData.get("clouds");

        // Tính mối tương quan giữa các yếu tố và lưu lượng giao thông
        double tempCorrelation = calculateCorrelation(temp, trafficVolume);
        double rainCorrelation = calculateCorrelation(rain, trafficVolume);
        double snowCorrelation = calculateCorrelation(snow, trafficVolume);
        double cloudsCorrelation = calculateCorrelation(clouds, trafficVolume);

        // In ra kết quả phân tích
        System.out.println("Mối tương quan giữa nhiệt độ và lưu lượng giao thông: " + tempCorrelation);
        System.out.println("Mối tương quan giữa mưa và lưu lượng giao thông: " + rainCorrelation);
        System.out.println("Mối tương quan giữa tuyết và lưu lượng giao thông: " + snowCorrelation);
        System.out.println("Mối tương quan giữa mây và lưu lượng giao thông: " + cloudsCorrelation);
    }

    // Hàm tính toán mối tương quan giữa hai danh sách (correlation)
    public static double calculateCorrelation(List<Double> x, List<Double> y) {
        double meanX = calculateMean(x);
        double meanY = calculateMean(y);

        double numerator = 0;
        double denominatorX = 0;
        double denominatorY = 0;

        for (int i = 0; i < x.size(); i++) {
            numerator += (x.get(i) - meanX) * (y.get(i) - meanY);
            denominatorX += Math.pow(x.get(i) - meanX, 2);
            denominatorY += Math.pow(y.get(i) - meanY, 2);
        }

        return numerator / (Math.sqrt(denominatorX) * Math.sqrt(denominatorY));
    }

    // Hàm tính trung bình của một danh sách
    public static double calculateMean(List<Double> data) {
        double sum = 0;
        for (double value : data) {
            sum += value;
        }
        return sum / data.size();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: TrafficWeatherAnalysis <input_path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];  // Đường dẫn đầu vào (ví dụ: /inputfolder1/data.csv)

        // Phân tích mối quan hệ giữa thời tiết và giao thông
        analyzeTrafficWeather(inputFilePath);
    }
}
