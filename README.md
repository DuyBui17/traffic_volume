import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class HolidayCorrection {

    // Mappersadasdasda
    public static class HolidayMapper extends Mapper<Object, Text, Text, Text> {
        private final Text dateKey = new Text();
        private final Text valueOutput = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header row
            if (fields[0].equals("traffic_volume")) {
                return;
            }

            // Extract date and mark as holiday or not
            String[] dateTimeParts = fields[8].split(" "); // Assuming datetime is in the 9th column
            String dateOnly = dateTimeParts[0]; // Extract date (dd-MM-yyyy)
            String holiday = fields[1]; // Holiday field (2nd column)

            dateKey.set(dateOnly); // Use the date as the key
            valueOutput.set(holiday + "," + line); // Include holiday info and the full line

            // Emit key-value pair
            context.write(dateKey, valueOutput);
        }
    }

    // Reducer Class
    public static class HolidayReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> lines = new HashSet<>();
            boolean isHoliday = false;

            // Check if any line in the same day is marked as holiday
            for (Text value : values) {
                String[] fields = value.toString().split(",", 2);
                if (!fields[0].equals("None")) {
                    isHoliday = true; // If any time in the day is a holiday
                }
                lines.add(fields[1]); // Add the rest of the line
            }

            // Write all lines with corrected holiday status
            for (String line : lines) {
                String[] fields = line.split(",", 9);
                fields[1] = isHoliday ? "Holiday" : "None"; // Update only the value of the holiday field
                result.set(String.join(",", fields));
                context.write(null, result); // Output corrected line
            }
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HolidayCorrection <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Holiday Correction");
        job.setJarByClass(HolidayCorrection.class);

        job.setMapperClass(HolidayMapper.class);
        job.setReducerClass(HolidayReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
