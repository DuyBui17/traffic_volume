import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

public class TempFilterJob1 {

    // Mapper class
    public static class TempFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text resultValue = new Text();
        private Text resultKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");

            if (data.length >= 3) {
                try {
                    // Đọc nhiệt độ (cột thứ 3)
                    double temp = Double.parseDouble(data[2]);
                    if (temp != -273.15) { // Bỏ qua các dòng có nhiệt độ không hợp lệ
                        // Ghi lại giá trị hợp lệ vào context
                        resultKey.set(data[0]);  // Có thể là mã thành phố hoặc ngày tháng
                        resultValue.set(line);   // Toàn bộ dòng dữ liệu
                        context.write(resultKey, resultValue);
                    }
                } catch (NumberFormatException e) {
                    // Bỏ qua dòng dữ liệu không hợp lệ
                }
            }
        }
    }

    // Reducer class
    public static class TempFilterReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Text resultValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Chỉ cần ghi tất cả các dòng hợp lệ vào output
            for (Text value : values) {
                resultValue.set(value);
                context.write(NullWritable.get(), resultValue); // Ghi kết quả vào file đầu ra
            }
        }
    }

    // Main class để cấu hình job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TempFilterJob1 <input path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[0];  // Đường dẫn đến file đầu vào (HDFS)
        String outputPath = args[1]; // Đường dẫn đến thư mục đầu ra (HDFS)

        // Cấu hình Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Filter Job");

        // Thiết lập lớp Mapper và Reducer
        job.setJarByClass(TempFilterJob1.class);
        job.setMapperClass(TempFilterMapper.class);
        job.setReducerClass(TempFilterReducer.class);

        // Cài đặt đầu ra của Map và Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Cài đặt đầu vào và đầu ra của job
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
