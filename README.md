import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TemperatureConverter {

    // Mapper Class: Chuyển đổi nhiệt độ từ Kelvin sang Celsius
    public static class TemperatureMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tách các trường dữ liệu bằng dấu phẩy
            String[] fields = value.toString().split(",");
            
            // Nếu không phải là dòng header, thực hiện chuyển đổi nhiệt độ
            if (fields.length > 2) {
                try {
                    // Lấy nhiệt độ từ trường thứ 3 (temp) trong Kelvin
                    double tempKelvin = Double.parseDouble(fields[2]);

                    // Chuyển đổi sang Celsius và làm tròn đến 2 chữ số thập phân
                    double tempCelsius = tempKelvin - 273.15;
                    tempCelsius = Math.round(tempCelsius * 100.0) / 100.0;

                    // Cập nhật lại nhiệt độ vào mảng dữ liệu
                    fields[2] = String.format("%.2f", tempCelsius);  // Định dạng lại nhiệt độ

                    // Tạo dòng kết quả mới và ghi vào context
                    context.write(new Text(String.join(",", fields)), new Text(""));  // Ghi cả dòng vào

                } catch (NumberFormatException e) {
                    // Nếu gặp lỗi khi phân tích nhiệt độ, bỏ qua
                    System.err.println("Error parsing temperature: " + e.getMessage());
                }
            }
        }
    }

    // Reducer Class: Ghi các dòng đã chuyển đổi vào file output
    public static class TemperatureReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Ghi các dòng đã được chuyển đổi vào tệp đầu ra
            context.write(key, new Text(""));  // Ghi toàn bộ dòng kết quả từ Mapper vào tệp đầu ra
        }
    }

    // Driver Class: Cấu hình và chạy job MapReduce
    public static class TemperatureConverterDriver {

        public static void main(String[] args) throws Exception {
            // Kiểm tra số lượng tham số
            if (args.length != 2) {
                System.err.println("Usage: TemperatureConverterDriver <input path> <output path>");
                System.exit(1);
            }

            // Cấu hình job Hadoop
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Temperature Converter");
            job.setJarByClass(TemperatureConverter.class);
            
            // Thiết lập Mapper và Reducer
            job.setMapperClass(TemperatureMapper.class);
            job.setReducerClass(TemperatureReducer.class);

            // Thiết lập định dạng đầu ra
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Thiết lập đường dẫn đầu vào và đầu ra
            FileInputFormat.addInputPath(job, new Path(args[0]));  // Đường dẫn tệp đầu vào
            FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Đường dẫn tệp đầu ra

            // Chạy job Hadoop
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
