import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Lớp chính cho chương trình MapReduce
public class TemperatureConverterMapReduce {

    // Mapper
    public static class TemperatureMapper extends Mapper<Object, Text, Text, Text> {

        // Hàm chuyển đổi từ Kelvin sang Celsius
        private double kelvinToCelsius(double kelvin) {
            return kelvin - 273.15;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Nếu là dòng header, phát nguyên dòng đó
            if (line.startsWith("header") || key.toString().equals("0")) {
                context.write(new Text(""), new Text(line));
                return;
            }

            String[] data = line.split(",");
            try {
                // Chuyển đổi nhiệt độ (cột thứ 3) từ Kelvin sang Celsius
                double kelvin = Double.parseDouble(data[2]);
                double celsius = kelvinToCelsius(kelvin);
                data[2] = String.format("%.2f", celsius); // Cập nhật giá trị mới

                // Tạo một chuỗi đã chỉnh sửa và phát ra
                context.write(new Text(""), new Text(String.join(",", data)));

            } catch (NumberFormatException e) {
                // Bỏ qua các dòng không hợp lệ
                System.err.println("Lỗi chuyển đổi dữ liệu: " + line);
            }
        }
    }

    // Reducer
    public static class TemperatureReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Ghi tất cả các giá trị (dòng đã xử lý) vào output
            for (Text value : values) {
                context.write(null, value);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TemperatureConverterMapReduce <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Converter");

        job.setJarByClass(TemperatureConverterMapReduce.class);

        // Cài đặt Mapper và Reducer
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        // Đầu ra của Mapper và Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Thiết lập input và output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
