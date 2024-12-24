import java.io.*;
import java.util.*;

public class TemperatureProcessor {

    // Phương thức chuyển đổi từ Fahrenheit sang Celsius
    public static double fahrenheitToCelsius(double fahrenheit) {
        return (fahrenheit - 32) * 5 / 9;
    }

    // Phương thức xử lý file CSV
    public static void processTemperatureCSV(String inputFile, String outputFile) {
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))
        ) {
            String line;
            boolean isHeader = true;

            // Đọc từng dòng trong file CSV
            while ((line = reader.readLine()) != null) {
                // Xử lý dòng tiêu đề
                if (isHeader) {
                    writer.write(line + "\n"); // Ghi tiêu đề vào file mới
                    isHeader = false;
                    continue;
                }

                // Phân tách các cột
                String[] columns = line.split(",");

                // Kiểm tra số lượng cột hợp lệ
                if (columns.length != 9) {
                    System.err.println("Invalid row: " + line);
                    continue;
                }

                // Chuyển đổi nhiệt độ từ Fahrenheit sang Celsius
                double tempFahrenheit = Double.parseDouble(columns[2]);
                double tempCelsius = fahrenheitToCelsius(tempFahrenheit);

                // Thay thế giá trị nhiệt độ trong cột `temp`
                columns[2] = String.format("%.2f", tempCelsius);

                // Ghi dòng đã chuyển đổi vào file mới
                writer.write(String.join(",", columns) + "\n");
            }

            System.out.println("Processing complete. Output saved to " + outputFile);

        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Tên file đầu vào và đầu ra
        String inputFile = "data.csv";
        String outputFile = "processed_data.csv";

        // Xử lý file CSV
        processTemperatureCSV(inputFile, outputFile);
    }
}
