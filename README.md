import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TrafficAnalysis {

    // Mapper class
    public static class TrafficMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text factorKey = new Text();
        private DoubleWritable trafficVolume = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");

            // Skip header row
            if (data[0].equalsIgnoreCase("traffic_volume")) {
                return;
            }

            try {
                // Extract relevant fields
                double traffic = Double.parseDouble(data[0]);
                String holiday = data[1];
                double temp = Double.parseDouble(data[2]);
                double rain = Double.parseDouble(data[3]);
                int clouds = Integer.parseInt(data[5]);

                // Emit key-value pairs for each factor
                trafficVolume.set(traffic);

                // Holiday factor
                factorKey.set("Holiday_" + holiday);
                context.write(factorKey, trafficVolume);

                // Temperature factor (rounded to nearest integer for grouping)
                factorKey.set("Temperature_" + Math.round(temp));
                context.write(factorKey, trafficVolume);

                // Rain factor
                factorKey.set("Rain_" + (rain > 0 ? "Yes" : "No"));
                context.write(factorKey, trafficVolume);

                // Cloud cover factor
                factorKey.set("Clouds_" + clouds);
                context.write(factorKey, trafficVolume);

            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignore invalid rows
            }
        }
    }

    // Reducer class
    public static class TrafficReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double totalTraffic = 0;
            int count = 0;

            // Aggregate traffic volume for each factor
            for (DoubleWritable value : values) {
                totalTraffic += value.get();
                count++;
            }

            // Compute average traffic volume
            if (count > 0) {
                result.set(totalTraffic / count);
                context.write(key, result);
            }
        }
    }

    // Main method to configure and run the job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TrafficAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Traffic Analysis");

        job.setJarByClass(TrafficAnalysis.class);
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
