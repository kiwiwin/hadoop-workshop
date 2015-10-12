package com.github.kiwi.hadoop.wordlengthmean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * NOTE: Combiner must match input and output to same Key/Value types of output from mapper.
 * <p/>
 * Example:
 * Mapper: [K_in, V_in, K_out, V_out]
 * Combiner: [k_out, V_out, K_out, V_out]
 * Reducer: [k_out, V_out, K_out, V_out]
 *
 * Do not assume that the combiner will run. Treat the combiner only as an optimization.
 */
public class WordLengthMean {
    public static class _Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                context.write(new Text("WordLengthMean"), new Text("" + tokenizer.nextToken().length() + "_" + 1));
            }
        }
    }

    public static class _Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for (Text aggregation : values) {
                final String[] split = aggregation.toString().split("_");
                sum += Long.parseLong(split[0]);
                count += Long.parseLong(split[1]);
            }
            context.write(key, new Text("" + sum + "_" + count));
        }
    }

    public static class _Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for (Text aggregation : values) {
                final String[] split = aggregation.toString().split("_");
                sum += Long.parseLong(split[0]);
                count += Long.parseLong(split[1]);
            }

            context.write(key, new Text(Double.toString(1.0 * sum / count)));
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration, "word length mean");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(_Mapper.class);
        job.setCombinerClass(_Combiner.class);
        job.setReducerClass(_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
