/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class rentMedianMapper extends Mapper<LongWritable, Text, Text, Text>
{

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {

    }

}
