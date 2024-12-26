import org.apache.hadoop.conf.Configuration; import org.apache.hadoop.fs.Path; import org.apache.hadoop.io.DoubleWritable; import org.apache.hadoop.io.Text; import org.apache.hadoop.mapreduce.Job; import org.apache.hadoop.mapreduce.Mapper; import org.apache.hadoop.mapreduce.Reducer; import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TrafficVolumeAnalysis {

// Mapper class
public static class TrafficMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable trafficVolume = new DoubleWritable();
    private final static Text holidayStatus = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Giả sử dữ liệu có dạng "traffic_volume,holiday,date_time"
        String[] fields = value.toString().split(",");

        // Kiểm tra số trường dữ liệu
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

                // Gửi dữ liệu đến Reducer
                context.write(holidayStatus, trafficVolume);
            } catch (NumberFormatException e) {
                // Nếu dữ liệu không hợp lệ, bỏ qua
                System.err.println("Skipping invalid record: " + value.toString());
            }
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

        double averageTraffic = totalTraffic / count;
        result.set(averageTraffic);

        // Gửi kết quả đến output
        context.write(key, result);
    }
}

// Main class để cấu hình job
public static void main(String[] args) throws Exception {
    // Cấu hình Hadoop job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Traffic Volume Analysis");

    // Thiết lập các lớp Mapper và Reducer
    job.setJarByClass(TrafficVolumeAnalysis.class);
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
