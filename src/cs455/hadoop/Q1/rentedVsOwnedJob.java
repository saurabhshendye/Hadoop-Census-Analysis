/**
 * Created by Saurabh on 3/28/2017.
 */

package cs455.hadoop.Q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


        }catch (IOException e)
        {
            // For job.getInstance
            e.printStackTrace();
        }



    }

}
