import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TemperatureConverter {

    // Mapper class
    public static class TempMapper extends Mapper<Object, Text, Text, Text> {

        private boolean isHeader = true;

        /**
         * Chuyển đổi từ Fahrenheit sang Celsius.
         *
         * @param fahrenheit nhiệt độ F
         * @return nhiệt độ C
         */
        private double fahrenheitToCelsius(double fahrenheit) {
            return (fahrenheit - 32) * 5 / 9;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Xử lý tiêu đề
            if (isHeader) {
                context.write(new Text(""), new Text(line)); // Ghi tiêu đề vào output
                isHeader = false;
                return;
            }

            // Phân tách dòng thành các cột
            String[] columns = line.split(",");

            // Kiểm tra số lượng cột hợp lệ
            if (columns.length != 9) {
                System.err.println("Invalid row: " + line);
                return;
            }

            try {
                // Chuyển đổi nhiệt độ
                double tempFahrenheit = Double.parseDouble(columns[2]);
                double tempCelsius = fahrenheitToCelsius(tempFahrenheit);
                columns[2] = String.format("%.2f", tempCelsius); // Cập nhật cột `temp`
            } catch (NumberFormatException e) {
                System.err.println("Lỗi chuyển đổi nhiệt độ ở dòng: " + line);
                return;
            }

            // Ghi dòng đã xử lý
            StringBuilder outputLine = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                outputLine.append(columns[i]);
                if (i < columns.length - 1) {
                    outputLine.append(",");
                }
            }
            context.write(new Text(""), new Text(outputLine.toString()));
        }
    }

    // Reducer class
    public static class TempReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(null, value); // Ghi tất cả các dòng vào output file
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Cấu hình job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Converter");
        job.setJarByClass(TemperatureConverter.class);

        // Thiết lập Mapper và Reducer
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        // Thiết lập định dạng đầu vào và đầu ra
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Đường dẫn input và output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
