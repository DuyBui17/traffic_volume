import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//0
import java.io.IOException;

public class TrafficVolume {

    // Mapper class
    public static class TrafficMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static DoubleWritable trafficVolume = new DoubleWritable();
        private final static Text holidayStatus = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Bỏ qua dòng tiêu đề
            if (line.startsWith("traffic_volume")) {
                return;
            }

            String[] fields = line.split(",");

            // Kiểm tra đủ 9 trường
            if (fields.length == 9) {
                try {
                    double traffic = Double.parseDouble(fields[0]); // Lấy traffic_volume
                    String holiday = fields[1]; // Lấy trạng thái holiday

                    // Thiết lập giá trị cho Mapper output
                    trafficVolume.set(traffic);
                    if ("None".equals(holiday)) {
                        holidayStatus.set("Non-Holiday");
                    } else {
                        holidayStatus.set("Holiday");
                    }

                    // Gửi dữ liệu đến Reducer
                    context.write(holidayStatus, trafficVolume);

                    // Debug log
                    System.out.println("Mapper output: " + holidayStatus.toString() + ", " + trafficVolume.get());
                } catch (NumberFormatException e) {
                    // Bỏ qua nếu dữ liệu không hợp lệ
                    System.err.println("Skipping invalid record: " + line);
                }
            } else {
                System.err.println("Invalid record format: " + line);
            }
        }
    }

    // Reducer class
    public static class TrafficReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final static DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalTraffic = 0.0;
            int count = 0;

            // Tính tổng và số lượng giá trị để tính trung bình
            for (DoubleWritable val : values) {
                totalTraffic += val.get();
                count++;
            }

            if (count > 0) {
                double averageTraffic = totalTraffic / count;
                result.set(averageTraffic);

                // Gửi kết quả đến output
                context.write(key, result);

                // Debug log
                System.out.println("Reducer output: " + key.toString() + ", " + result.get());
            } else {
                System.err.println("No values for key: " + key.toString());
            }
        }
    }

    // Main class để cấu hình job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Traffic Volume Analysis");

        // Thiết lập các lớp Mapper và Reducer
        job.setJarByClass(TrafficVolume.class);
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        // Định dạng dữ liệu output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Đọc dữ liệu từ input và ghi kết quả ra output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
