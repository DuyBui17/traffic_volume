public class TemperatureConverter {

    // Lớp Mapper
    public static class TemperatureMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 2) {
                try {
                    double tempKelvin = Double.parseDouble(fields[2]);
                    double tempCelsius = tempKelvin - 273.15;
                    tempCelsius = Math.round(tempCelsius * 100.0) / 100.0;
                    fields[2] = String.format("%.2f", tempCelsius);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < fields.length; i++) {
                        sb.append(fields[i]);
                        if (i < fields.length - 1) {
                            sb.append(",");
                        }
                    }
                    context.write(new Text(sb.toString()), new Text(""));
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing temperature: " + e.getMessage());
                }
            }
        }
    }

    // Lớp Reducer
    public static class TemperatureReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    // Lớp Driver (chứa phương thức main)
    public static class TemperatureConverterDriver {

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: TemperatureConverterDriver <input path> <output path>");
                System.exit(1);
            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Temperature Converter");
            job.setJarByClass(TemperatureConverterDriver.class); // Đảm bảo chỉ ra lớp chứa main
            
            job.setMapperClass(TemperatureMapper.class);
            job.setReducerClass(TemperatureReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
