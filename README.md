import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class TemperatureConverter {

    // Hàm chuyển từ K sang °C
    public static double kelvinToCelsius(double kelvin) {
        return kelvin - 273.15;
    }

    // Hàm nối các chuỗi với dấu phẩy
    public static String joinStrings(String[] data, String delimiter) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            sb.append(data[i]);
            if (i < data.length - 1) {
                sb.append(delimiter);  // Thêm dấu phân cách
            }
        }
        return sb.toString();
    }

    // Hàm xử lý đọc, chuyển đổi và ghi dữ liệu vào HDFS
    public static void convertTemperatureInHDFS(String inputFilePath, String outputDirPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Đọc file đầu vào từ HDFS
        Path inputPath = new Path(inputFilePath);
        FSDataInputStream in = fs.open(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        // Tạo output path
        Path outputPath = new Path(outputDirPath, "updated_data.csv");
        FSDataOutputStream out = fs.create(outputPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

        String line;
        boolean isFirstLine = true; // Biến để kiểm tra dòng đầu tiên (header)
        
        // Đọc từng dòng dữ liệu
        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");

            // Nếu là dòng header, chỉ ghi lại nguyên vẹn
            if (isFirstLine) {
                bw.write(joinStrings(data, ","));  // Sử dụng phương thức joinStrings
                bw.newLine();
                isFirstLine = false;
            } else {
                // Chuyển đổi nhiệt độ từ K sang °C
                double kelvin = Double.parseDouble(data[2]);
                double celsius = kelvinToCelsius(kelvin);

                // Cập nhật nhiệt độ mới vào dữ liệu
                data[2] = String.format("%.2f", celsius);  // Cập nhật cột "temp"

                // Ghi dữ liệu đã chuyển đổi vào file đầu ra
                bw.write(joinStrings(data, ","));  // Sử dụng phương thức joinStrings
                bw.newLine();
            }
        }

        // Đóng các dòng
        br.close();
        bw.close();
        in.close();
        out.close();

        System.out.println("Dữ liệu đã được ghi vào HDFS tại: " + outputDirPath + "/updated_data.csv");
    }

    public static void main(String[] args) throws Exception {
        // Lấy các tham số đầu vào từ dòng lệnh
        if (args.length != 2) {
            System.err.println("Usage: TemperatureConverter <input_path> <output_path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];  // Đường dẫn đầu vào (ví dụ: /inputfolder1/data.csv)
        String outputDirPath = args[1];  // Thư mục đầu ra trên HDFS (ví dụ: /out4)

        // Chuyển đổi nhiệt độ và lưu kết quả vào HDFS
        convertTemperatureInHDFS(inputFilePath, outputDirPath);
    }
}
