import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TrafficPeakHour {

    // Map Function: Phân loại dữ liệu theo giờ cao điểm 
    public static class TrafficMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Xác định các khoảng giờ cao điểm
        private static final int PEAK_MORNING_START = 7; // 7 AM
        private static final int PEAK_MORNING_END = 9;   // 9 AM
        private static final int PEAK_EVENING_START = 17; // 5 PM
        private static final int PEAK_EVENING_END = 19;   // 7 PM

        private final static IntWritable one = new IntWritable(1);
        private Text hourKey = new Text();

        // Hàm map
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Bỏ qua dòng tiêu đề (Giả sử dòng đầu tiên là tiêu đề)
            if (line.startsWith("traffic_volume")) {
                return;
            }

            String[] data = line.split(",");

            if (data.length >= 9) {
                try {
                    // Đọc giờ từ cột "date_time" (giả sử ở cột thứ 9)
                    String dateTime = data[8]; 
                    String hour = dateTime.split(" ")[1].split(":")[0]; // Lấy giờ từ cột date_time
                    int hourInt = Integer.parseInt(hour);

                    // Phân loại giờ cao điểm (7h-9h sáng, 17h-19h tối)
                    if ((hourInt >= PEAK_MORNING_START && hourInt <= PEAK_MORNING_END) ||
                        (hourInt >= PEAK_EVENING_START && hourInt <= PEAK_EVENING_END)) {
                        hourKey.set("Peak Hour");
                    } else {
                        hourKey.set("Non-Peak Hour");
                    }

                    // Gửi kết quả về reducer
                    context.write(hourKey, one);
                } catch (Exception e) {
                    // Bỏ qua dòng dữ liệu không hợp lệ
                    System.err.println("Skipping invalid record: " + line);
                }
            }
        }
    }

    // Reduce Function: Tổng hợp số lượng các sự kiện giao thông theo từng yếu tố 
    public static class TrafficReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        // Hàm reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();  // Tính tổng số sự kiện giao thông
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TrafficPeakHourAnalysis <input_path> <output_path>");
            System.exit(-1);
        }

        // Đọc các tham số đầu vào và đầu ra
        String inputPath = args[0];
        String outputPath = args[1];

        // Cấu hình cho job Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Traffic Peak Hour Analysis");
        job.setJarByClass(TrafficPeakHour.class);

        // Cài đặt Mapper và Reducer
        job.setMapperClass(TrafficMap.class);
        job.setReducerClass(TrafficReduce.class);

        // Cài đặt đầu ra của Map và Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Cài đặt đầu vào và đầu ra của job
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
