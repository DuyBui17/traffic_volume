import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TempFilter {

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

                    // Kiểm tra giá trị nhiệt độ, nếu không hợp lệ thì thay bằng giá trị mặc định
                    if (temp == -273.15) {
                        temp = 15.0; // Thay giá trị không hợp lệ bằng giá trị mặc định
                        data[2] = String.valueOf(temp); // Cập nhật lại nhiệt độ trong dữ liệu
                    }

                    // Ghi lại dữ liệu vào context
                    resultKey.set(data[0]);  // Có thể là mã thành phố hoặc ngày tháng
                    resultValue.set(String.join(",", data)); // Toàn bộ dòng dữ liệu đã thay đổi nếu cần
                    context.write(resultKey, resultValue);

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
        job.setJarByClass(TempFilter.class);
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
