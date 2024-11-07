// Required Hadoop and Java imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CategoryCountPrediction {

    // Mapper class for extracting year and category information
    public static class CategoryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text yearCategory = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), StandardCharsets.UTF_8); // Handle UTF-8 encoding
            String[] fields = line.split(","); // Assumes CSV is comma-separated

            // Ensure there are enough fields to avoid ArrayIndexOutOfBounds
            if (fields.length > 2) {
                String crawlDate = fields[0];
                String category = fields[1];

                // Extract year from the date
                String year = extractYear(crawlDate);
                if (year != null && !category.isEmpty()) {
                    yearCategory.set(year + ":" + category);
                    context.write(yearCategory, one);
                }
            }
        }

        private String extractYear(String crawlDate) {
            try {
                return crawlDate.substring(0, 4); // Extract the first 4 characters as the year
            } catch (Exception e) {
                return null;
            }
        }
    }

    // Reducer class for counting occurrences of each (year, category) pair
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Count Prediction");
        job.setJarByClass(CategoryCountPrediction.class);

        // Set mapper and reducer classes
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CountReducer.class);

        // Define output types for the Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Define output types for the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths from command line
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Specify input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

