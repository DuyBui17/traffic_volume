import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TemperatureConverter {

    public static class TempMapper extends Mapper<Object, Text, Text, Text> {

        private boolean isHeader = true;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header line
            if (isHeader) {
                isHeader = false;
                context.write(new Text(""), new Text(line));
                return;
            }

            String[] fields = line.split(",");
            if (fields.length > 2) {
                try {
                    double tempF = Double.parseDouble(fields[2]);
                    double tempC = tempF - 273.15; // Convert Kelvin to Celsius
                    fields[2] = String.format("%.2f", tempC);

                    // Manually construct the updated line
                    StringBuilder updatedLine = new StringBuilder();
                    for (int i = 0; i < fields.length; i++) {
                        updatedLine.append(fields[i]);
                        if (i < fields.length - 1) {
                            updatedLine.append(",");
                        }
                    }

                    // Write the updated record back
                    context.write(new Text(""), new Text(updatedLine.toString()));
                } catch (NumberFormatException e) {
                    // Skip invalid records
                }
            }
        }
    }

    public static class TempReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(null, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Converter");
        job.setJarByClass(TemperatureConverter.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Ensure output file is named explicitly
        Path outputFile = new Path(args[1] + "/temperature_converter.csv");
        FileOutputFormat.setOutputName(job, "temperature_converter");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
