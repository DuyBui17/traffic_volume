import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//ok
import java.io.IOException;

public class TempFilterJob {

    // Mapper Class
    public static class TempFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Bỏ qua dòng tiêu đề
            if (key.get() == 0 && line.contains("traffic_volume")) {
                context.write(new Text(""), new Text(line));
                return;
            }

            String[] data = line.split(",");

            try {
                // Kiểm tra giá trị trường `temp`
                double temp = Double.parseDouble(data[2]);
                if (temp == -273.15) {
                    return; // Bỏ qua dòng có giá trị ngoại lai
                }

                // Ghi lại các dòng hợp lệ
                context.write(new Text(""), new Text(String.join(",", data)));
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Bỏ qua các dòng không hợp lệ
            }
        }
    }

    // Reducer Class
    public static class TempFilterReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(null, value);
            }
        }
    }

    // Main Method (Driver)
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TempFilterJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Filter Job");

        job.setJarByClass(TempFilterJob.class);
        job.setMapperClass(TempFilterMapper.class);
        job.setReducerClass(TempFilterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
