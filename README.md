import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;

public class TrafficVolumeAnalysis {

    public static class TrafficMapper extends MapReduceBase implements Mapper<Object, Text, Text, DoubleWritable> {
        private final static DoubleWritable trafficVolume = new DoubleWritable();
        private final static Text holidayStatus = new Text();

        public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            // Giả sử dữ liệu có dạng "traffic_volume,holiday,date_time"
            String[] fields = value.toString().split(",");

            if (fields.length == 8) {
                try {
                    double traffic = Double.parseDouble(fields[0]);
                    String holiday = fields[1];  // "None" nếu không phải ngày lễ, "Holiday" nếu là ngày lễ
                    trafficVolume.set(traffic);

                    if ("None".equals(holiday)) {
                        holidayStatus.set("Non-Holiday");
                    } else {
                        holidayStatus.set("Holiday");
                    }

                    output.collect(holidayStatus, trafficVolume);
                } catch (NumberFormatException e) {
                    // Nếu dữ liệu không hợp lệ, bỏ qua
                    System.err.println("Skipping invalid record: " + value.toString());
                }
            }
        }
    }

    public static class TrafficReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final static DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double totalTraffic = 0.0;
            int count = 0;

            while (values.hasNext()) {
                totalTraffic += values.next().get();
                count++;
            }

            double averageTraffic = totalTraffic / count;
            result.set(averageTraffic);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf, TrafficVolumeAnalysis.class);
        job.setJobName("TrafficVolumeAnalysis");

        // Cấu hình mapper và reducer
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        // Cấu hình kiểu dữ liệu output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Đọc và ghi dữ liệu
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPaths(job, new Path(args[1]));

        // Chạy job Hadoop
        org.apache.hadoop.mapred.JobClient.runJob(job);
    }
}
