/**
 * Created by Saurabh on 3/28/2017.
 */

package cs455.hadoop.Q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class rentedVsOwnedJob
{
    public static void main(String [] args)
    {
        try
        {
            // Set all the configuration right. All configuration details are in conf directory
            Configuration conf = new Configuration();

            // Set the configuration and give job a name
            Job job = Job.getInstance(conf, "Rented vs Owned");

            // Current Job
            job.setJarByClass(rentedVsOwnedJob.class);

            // Set the Mapper class
            job.setMapperClass(rentedVsOwnedMapper.class);

            // Set the reducer class
            job.setReducerClass(rentedVsOwnedReducer.class);

            // Here we need to decide key value classes for mapper and reducer
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            // Path input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));

            // Path output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

        }
        catch (IOException e)
        {
            // For job.getInstance
            System.err.println(e.getMessage());
        }



    }

}
