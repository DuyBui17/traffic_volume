import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

public class TempFilterJob1 {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: TempFilterJob1 <input path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[0];  // Path to input file (HDFS)
        String outputPath = args[1]; // Path to output file (HDFS)

        // Get Hadoop configuration and file system
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Open input and output files
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
        OutputStream outputStream = fs.create(new Path(outputPath));
        PrintWriter writer = new PrintWriter(outputStream);

        String line;
        boolean isHeader = true;

        // Read the input file and process the data
        while ((line = reader.readLine()) != null) {
            // Skip the header row
            if (isHeader) {
                writer.println(line);  // Write header to output
                isHeader = false;
                continue;
            }

            String[] data = line.split(",");

            try {
                // Check temperature value
                double temp = Double.parseDouble(data[2]);
                if (temp == -273.15) {
                    continue; // Skip row with invalid temperature
                }

                // Join the data and write it to the output file
                StringBuilder valueBuilder = new StringBuilder();
                for (int i = 0; i < data.length; i++) {
                    valueBuilder.append(data[i]);
                    if (i < data.length - 1) {
                        valueBuilder.append(",");
                    }
                }
                writer.println(valueBuilder.toString());
                
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Skip invalid rows (e.g., missing temperature or parsing error)
            }
        }

        // Close the input and output streams
        reader.close();
        writer.close();

        System.out.println("Processing complete. Output saved to " + outputPath);
    }
}
