import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Locale;
import java.time.format.TextStyle;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200937
{

        public static class UberMapper extends Mapper<LongWritable, Text, Text, Text>
        {
                private Text region_day = new Text();
                private Text trips_vehicles = new Text();

                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                    //Write map code
                    String line = value.toString();
                    String[] uberInfo = line.split(",");
                    String region = uberInfo[0];
                    String date = uberInfo[1];
                    String vehicles = uberInfo[2];
                    String trips = uberInfo[3];

                    // 1/15/2015 
                    String[] dateInfo = date.split("/");
                    String month = dateInfo[0];
                    String day = dateInfo[1];
                    String year = dateInfo[2];
                    LocalDate date2 = LocalDate.of( Integer.parseInt(year),  Integer.parseInt(month),  Integer.parseInt(day));
                    DayOfWeek dayOfWeek = date2.getDayOfWeek();
                    String dayofweek = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();
                    if(dayofweek.equals("THU")) {
                            dayofweek = "THR";
                    }
                    String region_day1 = region+","+dayofweek;
                    String trips_vehicles1 = trips+","+vehicles;

                    region_day.set(region_day1);
                    trips_vehicles.set(trips_vehicles1);
                    context.write(region_day, trips_vehicles);
                }
        }

        public static class UberReducer extends Reducer<Text,Text,Text,Text>
        {
                private int tripSum = 0;
                private int vehicleSum = 0;
                private Text trips_vehicles = new Text();

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                    int tripSum = 0;
                    int vehicleSum = 0;
                    //Write reduce code
                    for (Text val : values) {
                        String line = val.toString();
                        String[] trip_vehicle = line.split(",");
                        
                        String trip_string = trip_vehicle[0];
                        String vehicle_string = trip_vehicle[1];

                        int trip_int = Integer.parseInt(trip_string);
                        int vehicle_int = Integer.parseInt(vehicle_string);

                        tripSum += trip_int;
                        vehicleSum += vehicle_int;
                    }
                    String t_v = Integer.toString(tripSum)+","+Integer.toString(vehicleSum);
                    trips_vehicles.set(t_v);
                    context.write(key, trips_vehicles);
                }
        }

        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2)
                {
                        System.err.println("Usage: Uber <in> <out>");
                        System.exit(2);
                }
                Job job = new Job(conf, "Uber");
                job.setJarByClass(UBERStudent20200937.class);
                job.setMapperClass(UberMapper.class);
                job.setCombinerClass(UberReducer.class);
                job.setReducerClass(UberReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
