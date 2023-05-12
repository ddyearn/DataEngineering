import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200937 {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);

        private Text movie_genre = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Write map code
            String line = value.toString();
            String[] movieInfo = line.split("::");
            String id = movieInfo[0];
            String title = movieInfo[1];
            String genre = movieInfo[2];
            StringTokenizer tokenizer = new StringTokenizer(genre, "|");
            while (tokenizer.hasMoreTokens()) {
                movie_genre.set(tokenizer.nextToken());
                context.write(movie_genre, one);
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sumWritable = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Write reduce code
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            sumWritable.set(sum);
            context.write(key, sumWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: movie <in> <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Movie");

        job.setJarByClass(IMDBStudent20200937.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapOutputKeyClass(DoubleString.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[2]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
