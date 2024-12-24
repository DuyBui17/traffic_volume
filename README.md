import java.io.*;

public class TemperatureConverterFromCSV {

    /**
     * Chuyển đổi nhiệt độ từ Fahrenheit sang Celsius.
     *
     * @param fahrenheit nhiệt độ F
     * @return nhiệt độ C
     */
    public static double fahrenheitToCelsius(double fahrenheit) {
        return (fahrenheit - 32) * 5 / 9;
    }

    /**
     * Xử lý file CSV, chuyển đổi cột nhiệt độ từ Fahrenheit sang Celsius
     * và lưu kết quả vào file mới.
     *
     * @param inputFile  tên file đầu vào
     * @param outputFile tên file đầu ra
     */
    public static void processTemperatureCSV(String inputFile, String outputFile) {
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))
        ) {
            String line;
            boolean isHeader = true;

            // Đọc từng dòng trong file CSV
            while ((line = reader.readLine()) != null) {
                // Ghi dòng tiêu đề vào file mới
                if (isHeader) {
                    writer.write(line + System.lineSeparator());
                    isHeader = false;
                    continue;
                }

                // Phân tách các cột
                String[] columns = line.split(",");

                // Kiểm tra số lượng cột hợp lệ
                if (columns.length != 9) {
                    System.err.println("Dòng không hợp lệ: " + line);
                    continue;
                }

                // Chuyển đổi nhiệt độ từ Fahrenheit sang Celsius
                try {
                    double tempFahrenheit = Double.parseDouble(columns[2]);
                    double tempCelsius = fahrenheitToCelsius(tempFahrenheit);
                    columns[2] = String.format("%.2f", tempCelsius); // Cập nhật giá trị trong cột `temp`
                } catch (NumberFormatException e) {
                    System.err.println("Lỗi chuyển đổi nhiệt độ ở dòng: " + line);
                }

                // Xây dựng lại dòng và ghi vào file mới
                StringBuilder rowBuilder = new StringBuilder();
                for (int i = 0; i < columns.length; i++) {
                    rowBuilder.append(columns[i]);
                    if (i < columns.length - 1) {
                        rowBuilder.append(","); // Thêm dấu phẩy giữa các cột
                    }
                }
                writer.write(rowBuilder.toString() + System.lineSeparator());
            }

            System.out.println("Xử lý hoàn tất. File đầu ra: " + outputFile);

        } catch (IOException e) {
            System.err.println("Lỗi khi xử lý file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Tên file đầu vào và đầu ra
        String inputFile = "data.csv";
        String outputFile = "processed_data.csv";

        // Gọi phương thức xử lý file CSV
        processTemperatureCSV(inputFile, outputFile);
    }
}
