import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Mapper Class
public class TrafficAnalysis {
    public static class TrafficMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text hourKey = new Text();
        private IntWritable trafficVolume = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the CSV line
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length < 9) {
                return; // Skip invalid rows
            }

            try {
                String dateTime = fields[8]; // Date_time column
                int traffic = Integer.parseInt(fields[0]); // Traffic_volume column

                // Extract hour from date_time (format assumed: "02-10-2012 09:00")
                String hour = dateTime.split(" ")[1].split(":")[0];

                hourKey.set(hour); // Set hour as key
                trafficVolume.set(traffic); // Set traffic volume as value

                context.write(hourKey, trafficVolume);
            } catch (Exception e) {
                // Handle any parsing errors
                e.printStackTrace();
            }
        }
    }

    // Reducer Class
    public static class TrafficReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalTraffic = 0;
            int count = 0;

            // Calculate total and count for the current hour
            for (IntWritable value : values) {
                totalTraffic += value.get();
                count++;
            }

            double averageTraffic = count == 0 ? 0 : (double) totalTraffic / count;

            // Output the total and average traffic for the hour
            String result = "Total: " + totalTraffic + ", Average: " + String.format("%.2f", averageTraffic);
            context.write(key, new Text(result));
        }
    }

    // Main Method to Configure and Run the Job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Traffic Analysis by Hour");
        job.setJarByClass(TrafficAnalysis.class);

        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
